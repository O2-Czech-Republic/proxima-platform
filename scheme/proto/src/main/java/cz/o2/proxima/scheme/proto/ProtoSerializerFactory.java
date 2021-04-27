/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.scheme.proto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.Parser;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.scheme.AttributeValueAccessor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.scheme.proto.transactions.Transactions;
import cz.o2.proxima.scheme.proto.transactions.Transactions.ProtoRequest;
import cz.o2.proxima.scheme.proto.transactions.Transactions.ProtoResponse;
import cz.o2.proxima.scheme.proto.transactions.Transactions.ProtoState;
import cz.o2.proxima.scheme.proto.utils.ProtoUtils;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.transaction.TransactionSerializerSchemeProvider;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.util.ExceptionUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Serializer from protobuffers. */
@Slf4j
public class ProtoSerializerFactory implements ValueSerializerFactory {

  private static final long serialVersionUID = 1L;

  private final Map<URI, ValueSerializer<?>> parsers = new ConcurrentHashMap<>();

  @Override
  public String getAcceptableScheme() {
    return "proto";
  }

  @SuppressWarnings("unchecked")
  private static <M extends AbstractMessage> ValueSerializer<M> createSerializer(URI uri) {
    String className = uri.getSchemeSpecificPart();
    if (className.startsWith("cz.o2.proxima.transaction.")) {
      return TransactionProtoSerializer.ofTransactionClass(className);
    }
    return new ProtoValueSerializer<>(className);
  }

  @SuppressWarnings("unchecked")
  static <M extends AbstractMessage> M getDefaultInstance(String protoClass) {
    try {
      Class<? extends AbstractMessage> cls = Classpath.findClass(protoClass, AbstractMessage.class);
      Method method = cls.getMethod("getDefaultInstance");
      return (M) method.invoke(null);
    } catch (Exception ex) {
      throw new IllegalArgumentException(
          "Cannot retrieve default instance for type " + protoClass, ex);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ValueSerializer<T> getValueSerializer(URI scheme) {
    return (ValueSerializer<T>)
        parsers.computeIfAbsent(scheme, ProtoSerializerFactory::createSerializer);
  }

  @Override
  public boolean canProvideTransactionSerializer() {
    return true;
  }

  @Override
  public TransactionSerializerSchemeProvider createTransactionSerializerSchemeProvider() {
    return TransactionSerializerSchemeProvider.of(
        "proto:" + Request.class.getName(),
        "proto:" + Response.class.getName(),
        "proto:" + State.class.getName());
  }

  private static class ProtoValueSerializer<MessageT extends AbstractMessage>
      implements ValueSerializer<MessageT> {

    private static final long serialVersionUID = 1L;

    final String protoClass;
    @Nullable transient MessageT defVal = null;

    transient Parser<?> parser = null;

    @Nullable private transient SchemaTypeDescriptor<MessageT> valueSchemaDescriptor;

    @Nullable private transient ProtoMessageValueAccessor<MessageT> accessor;

    ProtoValueSerializer(Class<MessageT> protoClass) {
      this(protoClass.getName());
    }

    ProtoValueSerializer(String protoClass) {
      this.protoClass = protoClass;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Optional<MessageT> deserialize(byte[] input) {
      if (parser == null) {
        parser = getParserForClass(protoClass);
      }
      try {
        return Optional.of((MessageT) parser.parseFrom(input));
      } catch (Exception ex) {
        log.debug("Failed to parse input bytes", ex);
      }
      return Optional.empty();
    }

    @Override
    public MessageT getDefault() {
      if (defVal == null) {
        defVal = getDefaultInstance(protoClass);
      }
      return defVal;
    }

    @Override
    public byte[] serialize(MessageT value) {
      return value.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private Parser<?> getParserForClass(String protoClassName) {

      try {
        Class<?> proto = Classpath.findClass(protoClassName, AbstractMessage.class);
        Method p = proto.getMethod("parser");
        return (Parser<?>) p.invoke(null);
      } catch (IllegalAccessException
          | IllegalArgumentException
          | NoSuchMethodException
          | SecurityException
          | InvocationTargetException ex) {

        throw new IllegalArgumentException("Cannot create parser from class " + protoClassName, ex);
      }
    }

    @Override
    public String getLogString(MessageT value) {
      return TextFormat.shortDebugString(value);
    }

    @Override
    public String asJsonValue(MessageT value) {
      return ExceptionUtils.uncheckedFactory(() -> JsonFormat.printer().print(value));
    }

    @SuppressWarnings("unchecked")
    @Override
    public MessageT fromJsonValue(String json) {
      Builder builder = getDefault().toBuilder();
      ExceptionUtils.unchecked(() -> JsonFormat.parser().merge(json, builder));
      return (MessageT) builder.build();
    }

    @Override
    public SchemaTypeDescriptor<MessageT> getValueSchemaDescriptor() {
      if (valueSchemaDescriptor == null) {
        valueSchemaDescriptor =
            ProtoUtils.convertProtoToSchema(getDefault().getDescriptorForType());
      }
      return valueSchemaDescriptor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <OutputT> AttributeValueAccessor<MessageT, OutputT> getValueAccessor() {
      if (accessor == null) {
        accessor = new ProtoMessageValueAccessor<>(this::getDefault);
      }
      return (AttributeValueAccessor<MessageT, OutputT>) accessor;
    }
  }

  @VisibleForTesting
  static class TransactionProtoSerializer<TransactionT, ProtoTransactionT extends AbstractMessage>
      implements ValueSerializer<TransactionT>, ValueSerializer.InitializedWithRepository {

    @SuppressWarnings({"unchecked", "rawtypes"})
    static <V, P extends AbstractMessage> TransactionProtoSerializer<V, P> ofTransactionClass(
        String className) {

      switch (className) {
        case "cz.o2.proxima.transaction.Request":
          return (TransactionProtoSerializer)
              new TransactionProtoSerializer<Request, ProtoRequest>(
                  new ProtoValueSerializer<>(ProtoRequest.class.getName()),
                  TransactionProtoSerializer::requestToProto,
                  TransactionProtoSerializer::requestFromProto);
        case "cz.o2.proxima.transaction.Response":
          return (TransactionProtoSerializer)
              new TransactionProtoSerializer<Response, ProtoResponse>(
                  new ProtoValueSerializer<>(ProtoResponse.class.getName()),
                  TransactionProtoSerializer::responseToProto,
                  TransactionProtoSerializer::responseFromProto);
        case "cz.o2.proxima.transaction.State":
          return (TransactionProtoSerializer)
              new TransactionProtoSerializer<State, ProtoState>(
                  new ProtoValueSerializer<>(ProtoState.class.getName()),
                  TransactionProtoSerializer::stateToProto,
                  TransactionProtoSerializer::stateFromProto);
      }
      throw new UnsupportedOperationException("Unknown className of transactions: " + className);
    }

    private static State stateFromProto(Repository repository, ProtoState state) {
      switch (state.getFlags()) {
        case UNKNOWN:
          return State.empty();
        case OPEN:
          return State.open(
              state.getSeqId(),
              new HashSet<>(getKeyAttributesFromProto(repository, state.getInputAttributesList())));
        case COMMITTED:
          return State.open(
                  state.getSeqId(),
                  new HashSet<>(
                      getKeyAttributesFromProto(repository, state.getInputAttributesList())))
              .committed(
                  new HashSet<>(
                      getKeyAttributesFromProto(repository, state.getCommittedAttributesList())));
        case ABORTED:
          return State.open(
                  state.getSeqId(),
                  new HashSet<>(
                      getKeyAttributesFromProto(repository, state.getInputAttributesList())))
              .aborted();
        default:
          throw new IllegalStateException("Unknown flags: " + state.getFlags());
      }
    }

    private static ProtoState stateToProto(Repository repository, State state) {
      return ProtoState.newBuilder()
          .setFlags(asFlags(state.getFlags()))
          .addAllInputAttributes(asProtoKeyAttributes(state.getInputAttributes()))
          .addAllCommittedAttributes(asProtoKeyAttributes(state.getCommittedAttributes()))
          .setSeqId(state.getSequentialId())
          .build();
    }

    private static Response responseFromProto(Repository repository, ProtoResponse response) {
      switch (response.getFlags()) {
        case UNKNOWN:
          return Response.empty();
        case OPEN:
          return Response.open(response.getSeqId());
        case COMMITTED:
          return Response.committed();
        case ABORTED:
          return Response.aborted();
        case DUPLICATE:
          return Response.duplicate();
        case UPDATE:
          return Response.updated();
        default:
          throw new IllegalArgumentException("Unknown flag: " + response.getFlags());
      }
    }

    private static ProtoResponse responseToProto(Repository repository, Response response) {
      return ProtoResponse.newBuilder()
          .setFlags(asFlags(response.getFlags()))
          .setSeqId(response.hasSequenceId() ? response.getSeqId() : 0L)
          .build();
    }

    private static ProtoRequest requestToProto(Repository repository, Request request) {
      return ProtoRequest.newBuilder()
          .addAllInputAttribute(asProtoKeyAttributes(request.getInputAttributes()))
          .addAllOutputAttribute(asProtoKeyAttributes(request.getOutputAttributes()))
          .setFlags(asFlags(request.getFlags()))
          .build();
    }

    private static Request requestFromProto(Repository repository, ProtoRequest protoRequest) {
      return Request.builder()
          .inputAttributes(
              getKeyAttributesFromProto(repository, protoRequest.getInputAttributeList()))
          .outputAttributes(
              getKeyAttributesFromProto(repository, protoRequest.getOutputAttributeList()))
          .flags(asFlags(protoRequest.getFlags()))
          .build();
    }

    private static Request.Flags asFlags(Transactions.Flags flags) {
      switch (flags) {
        case UNKNOWN:
          return Request.Flags.NONE;
        case OPEN:
          return Request.Flags.OPEN;
        case COMMITTED:
          return Request.Flags.COMMIT;
        case UPDATE:
          return Request.Flags.UPDATE;
        case ROLLBACK:
          return Request.Flags.ROLLBACK;
        default:
          throw new IllegalArgumentException("Unknown flags: " + flags);
      }
    }

    private static Transactions.Flags asFlags(Response.Flags flags) {
      switch (flags) {
        case NONE:
          return Transactions.Flags.UNKNOWN;
        case OPEN:
          return Transactions.Flags.OPEN;
        case COMMITTED:
          return Transactions.Flags.COMMITTED;
        case ABORTED:
          return Transactions.Flags.ABORTED;
        case DUPLICATE:
          return Transactions.Flags.DUPLICATE;
        case UPDATED:
          return Transactions.Flags.UPDATE;
        default:
          throw new IllegalArgumentException("Unknown flags: " + flags);
      }
    }

    private static Transactions.Flags asFlags(State.Flags flags) {
      switch (flags) {
        case UNKNOWN:
          return Transactions.Flags.UNKNOWN;
        case OPEN:
          return Transactions.Flags.OPEN;
        case COMMITTED:
          return Transactions.Flags.COMMITTED;
        case ABORTED:
          return Transactions.Flags.ABORTED;
        default:
          throw new IllegalArgumentException("Unknown flags: " + flags);
      }
    }

    private static Transactions.Flags asFlags(Request.Flags flags) {
      switch (flags) {
        case NONE:
          return Transactions.Flags.UNKNOWN;
        case OPEN:
          return Transactions.Flags.OPEN;
        case COMMIT:
          return Transactions.Flags.COMMITTED;
        case UPDATE:
          return Transactions.Flags.UPDATE;
        case ROLLBACK:
          return Transactions.Flags.ROLLBACK;
        default:
          throw new IllegalArgumentException("Unknown flags: " + flags);
      }
    }

    private static List<KeyAttribute> getKeyAttributesFromProto(
        Repository repo, List<Transactions.KeyAttribute> attrList) {

      return attrList
          .stream()
          .map(
              a -> {
                EntityDescriptor entity = repo.getEntity(a.getEntity());
                if (Strings.isNullOrEmpty(a.getAttribute())) {
                  return KeyAttribute.ofAttributeDescriptor(
                      entity, a.getKey(), entity.getAttribute(a.getAttributeDesc()), a.getSeqId());
                }
                return KeyAttribute.ofAttributeDescriptor(
                    entity,
                    a.getKey(),
                    entity.getAttribute(a.getAttributeDesc()),
                    a.getSeqId(),
                    a.getAttribute());
              })
          .collect(Collectors.toList());
    }

    private static Iterable<Transactions.KeyAttribute> asProtoKeyAttributes(
        Iterable<KeyAttribute> inputAttributes) {

      return Iterables.transform(
          inputAttributes,
          a ->
              Transactions.KeyAttribute.newBuilder()
                  .setEntity(a.getEntity().getName())
                  .setAttributeDesc(a.getAttributeDescriptor().getName())
                  .setKey(a.getKey())
                  .setAttribute(a.getAttribute().orElse(""))
                  .setSeqId(a.getSequenceId())
                  .build());
    }

    private final ProtoValueSerializer<ProtoTransactionT> inner;
    private final BiFunction<Repository, TransactionT, ProtoTransactionT> asMessage;
    private final BiFunction<Repository, ProtoTransactionT, TransactionT> asTransaction;
    private RepositoryFactory repoFactory;
    private transient Repository repo;

    public TransactionProtoSerializer(
        ProtoValueSerializer<ProtoTransactionT> inner,
        BiFunction<Repository, TransactionT, ProtoTransactionT> asMessage,
        BiFunction<Repository, ProtoTransactionT, TransactionT> asTransaction) {

      this.inner = inner;
      this.asMessage = asMessage;
      this.asTransaction = asTransaction;
    }

    @Override
    public Optional<TransactionT> deserialize(byte[] input) {
      return inner.deserialize(input).map(m -> asTransaction.apply(repo(), m));
    }

    @Override
    public byte[] serialize(TransactionT value) {
      return inner.serialize(asMessage.apply(repo(), value));
    }

    @Override
    public TransactionT getDefault() {
      return asTransaction.apply(repo(), inner.getDefault());
    }

    private Repository repo() {
      if (repo == null && repoFactory != null) {
        repo = repoFactory.apply();
      }
      return repo;
    }

    @Override
    public void setRepository(Repository repository) {
      this.repo = repository;
      this.repoFactory = repo.asFactory();
    }
  }
}
