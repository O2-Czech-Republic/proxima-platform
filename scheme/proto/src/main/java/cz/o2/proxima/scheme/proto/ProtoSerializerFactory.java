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
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.Parser;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.scheme.AttributeValueAccessor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.scheme.proto.transactions.Transactions.ProtoRequest;
import cz.o2.proxima.scheme.proto.transactions.Transactions.ProtoResponse;
import cz.o2.proxima.scheme.proto.transactions.Transactions.ProtoState;
import cz.o2.proxima.scheme.proto.utils.ProtoUtils;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;
import cz.o2.proxima.transaction.TransactionSerializerSchemeProvider;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.util.ExceptionUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
  static class TransactionProtoSerializer<T> implements ValueSerializer<T> {

    @SuppressWarnings("unchecked")
    static <V> TransactionProtoSerializer<V> ofTransactionClass(String className) {
      switch (className) {
        case "cz.o2.proxima.transaction.Request":
          return (TransactionProtoSerializer<V>)
              new TransactionProtoSerializer<>(
                  new ProtoValueSerializer<>(ProtoRequest.class.getName()),
                  req -> ProtoRequest.newBuilder().build(),
                  req -> Request.of());
        case "cz.o2.proxima.transaction.Response":
          return (TransactionProtoSerializer<V>)
              new TransactionProtoSerializer<>(
                  new ProtoValueSerializer<>(ProtoResponse.class.getName()),
                  req -> ProtoResponse.newBuilder().build(),
                  req -> Response.of());
        case "cz.o2.proxima.transaction.State":
          return (TransactionProtoSerializer<V>)
              new TransactionProtoSerializer<>(
                  new ProtoValueSerializer<>(ProtoState.class.getName()),
                  req -> ProtoState.newBuilder().build(),
                  req -> State.of());
      }
      throw new UnsupportedOperationException("Unknown className of transactions: " + className);
    }

    private final ProtoValueSerializer<AbstractMessage> inner;
    private final UnaryFunction<T, AbstractMessage> asMessage;
    private final UnaryFunction<AbstractMessage, T> asTransaction;

    public TransactionProtoSerializer(
        ProtoValueSerializer<AbstractMessage> inner,
        UnaryFunction<T, AbstractMessage> asMessage,
        UnaryFunction<AbstractMessage, T> asTransaction) {

      this.inner = inner;
      this.asMessage = asMessage;
      this.asTransaction = asTransaction;
    }

    @Override
    public Optional<T> deserialize(byte[] input) {
      return inner.deserialize(input).map(asTransaction::apply);
    }

    @Override
    public byte[] serialize(T value) {
      return inner.serialize(asMessage.apply(value));
    }

    @Override
    public T getDefault() {
      return asTransaction.apply(inner.getDefault());
    }
  }
}
