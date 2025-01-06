/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.server;

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.KeyAttribute;
import cz.o2.proxima.core.transaction.KeyAttributes;
import cz.o2.proxima.core.transaction.Response.Flags;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader.Listing;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.server.metrics.Metrics;
import cz.o2.proxima.direct.server.rpc.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.GetRequest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.GetResponse;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.ListRequest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.ListResponse;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.MultifetchRequest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.MultifetchResponse;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.ScanRequest;
import cz.o2.proxima.direct.server.rpc.proto.service.Rpc.ScanResult;
import cz.o2.proxima.direct.server.transaction.TransactionContext;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Service for reading data. */
@Slf4j
public class RetrieveService extends RetrieveServiceGrpc.RetrieveServiceImplBase {

  private final Map<AttributeDescriptor<?>, RandomAccessReader> readerMap;
  private final Repository repo;
  private final DirectDataOperator direct;
  private final TransactionContext transactionContext;

  public RetrieveService(
      Repository repo, DirectDataOperator direct, TransactionContext transactionContext) {

    this.repo = repo;
    this.direct = direct;
    this.readerMap = Collections.synchronizedMap(new HashMap<>());
    this.transactionContext = transactionContext;
  }

  private static class Status extends Exception {
    final int statusCode;
    final String message;

    Status(int statusCode, String message) {
      this.statusCode = statusCode;
      this.message = message;
    }
  }

  @Override
  public void begin(
      Rpc.BeginTransactionRequest request,
      StreamObserver<Rpc.BeginTransactionResponse> responseObserver) {

    String transactionId = transactionContext.create(request.getTranscationId());
    responseObserver.onNext(
        Rpc.BeginTransactionResponse.newBuilder().setTransactionId(transactionId).build());
    responseObserver.onCompleted();
  }

  @Override
  public void listAttributes(
      Rpc.ListRequest request, StreamObserver<Rpc.ListResponse> responseObserver) {

    try {
      Metrics.LIST_REQUESTS.increment();
      if (request.getEntity().isEmpty()
          || request.getKey().isEmpty()
          || request.getWildcardPrefix().isEmpty()) {
        throw new Status(400, "Missing some required fields");
      }

      if (!request.getTransactionId().isEmpty()
          && (!request.getOffset().isEmpty() || request.getLimit() > 0)) {
        throw new Status(
            400, "Unsupported: transactions do not support limited list requests, currently");
      }

      EntityDescriptor entity =
          repo.findEntity(request.getEntity())
              .orElseThrow(() -> new Status(404, "Entity " + request.getEntity() + " not found"));

      AttributeDescriptor<Object> wildcard =
          entity
              .findAttribute(request.getWildcardPrefix() + ".*")
              .orElseThrow(
                  () ->
                      new Status(
                          404,
                          "Entity "
                              + request.getEntity()
                              + " does not have wildcard attribute "
                              + request.getWildcardPrefix()));

      RandomAccessReader reader = instantiateReader(wildcard);

      Rpc.ListResponse.Builder response = Rpc.ListResponse.newBuilder().setStatus(200);
      Predicate<KeyValue<Object>> filterPredicate =
          request.getWildcardPrefix().equals(wildcard.toAttributePrefix())
                  || request.getWildcardPrefix().equals(wildcard.toAttributePrefix(false))
              ? ign -> true
              : kv -> kv.getAttribute().startsWith(request.getWildcardPrefix());
      List<KeyValue<Object>> kvs = processListRequest(request, wildcard, reader, filterPredicate);
      kvs.forEach(
          kv ->
              response.addValue(
                  ListResponse.AttrValue.newBuilder()
                      .setAttribute(kv.getAttribute())
                      .setValue(ByteString.copyFrom(kv.getValue()))));
      noticeListResult(request, entity, wildcard, kvs);
      replyLogged(responseObserver, request, response.build());
    } catch (Status s) {
      replyStatusLogged(responseObserver, request, s.statusCode, s.message);
    } catch (Exception ex) {
      log.error("Failed to process request {}", request, ex);
      replyStatusLogged(responseObserver, request, 500, ex.getMessage());
    }
    responseObserver.onCompleted();
  }

  private static List<KeyValue<Object>> processListRequest(
      ListRequest request,
      AttributeDescriptor<Object> wildcard,
      RandomAccessReader reader,
      Predicate<KeyValue<Object>> filterPredicate)
      throws Status {

    List<KeyValue<Object>> kvs = new ArrayList<>();
    String requestOffset = request.getOffset();
    if (requestOffset.isEmpty()) {
      requestOffset = request.getWildcardPrefix();
    }

    if (!requestOffset.startsWith(request.getWildcardPrefix())) {
      throw new Status(
          400,
          String.format(
              "Offset must have prefix given by wildcardPrefix, got %s and %s",
              requestOffset, request.getWildcardPrefix()));
    }
    boolean prefixed = request.getWildcardPrefix().length() > wildcard.toAttributePrefix().length();
    int limit = request.getLimit() > 0 ? request.getLimit() : -1;
    int requestLimit = limit;
    if (prefixed && limit == -1) {
      requestLimit = 100;
    }
    AtomicReference<RandomOffset> effectiveOffset =
        new AtomicReference<>(reader.fetchOffset(Listing.ATTRIBUTE, requestOffset));
    do {
      AtomicBoolean failedPredicate = new AtomicBoolean();
      AtomicInteger addedKvs = new AtomicInteger();
      synchronized (reader) {
        reader.scanWildcard(
            request.getKey(),
            wildcard,
            effectiveOffset.get(),
            requestLimit,
            kv -> {
              effectiveOffset.set(kv.getOffset());
              if (filterPredicate.test(kv)) {
                kvs.add(kv);
                addedKvs.incrementAndGet();
              } else {
                failedPredicate.set(true);
              }
            });
      }
      if (addedKvs.get() == 0 || failedPredicate.get()) {
        break;
      }
    } while (limit > 0 && kvs.size() < limit);
    return kvs;
  }

  private static void replyStatusLogged(
      StreamObserver<Rpc.ListResponse> responseObserver,
      MessageOrBuilder request,
      int statusCode,
      String message) {

    replyLogged(
        responseObserver,
        request,
        Rpc.ListResponse.newBuilder().setStatus(statusCode).setStatusMessage(message).build());
  }

  private static void replyLogged(
      StreamObserver<Rpc.ListResponse> responseObserver,
      MessageOrBuilder request,
      Rpc.ListResponse response) {
    logStatus("listAttributes", request, response.getStatus(), response.getStatusMessage());
    responseObserver.onNext(response);
  }

  @Override
  public void get(Rpc.GetRequest request, StreamObserver<Rpc.GetResponse> responseObserver) {

    Metrics.GET_REQUESTS.increment();

    try {
      if (request.getEntity().isEmpty()
          || request.getKey().isEmpty()
          || request.getAttribute().isEmpty()) {
        throw new Status(400, "Missing some required fields");
      }

      EntityDescriptor entity =
          repo.findEntity(request.getEntity())
              .orElseThrow(() -> new Status(404, "Entity " + request.getEntity() + " not found"));

      AttributeDescriptor<Object> attribute =
          entity
              .findAttribute(request.getAttribute())
              .orElseThrow(
                  () ->
                      new Status(
                          404,
                          "Entity "
                              + request.getEntity()
                              + " does not have attribute "
                              + request.getAttribute()));

      RandomAccessReader reader = instantiateReader(attribute);

      synchronized (reader) {
        Optional<KeyValue<Object>> maybeValue =
            reader.get(request.getKey(), request.getAttribute(), attribute);

        noticeGetResult(request, entity, attribute, maybeValue);
        KeyValue<Object> kv =
            maybeValue.orElseThrow(
                () ->
                    new Status(
                        404,
                        "Key "
                            + request.getKey()
                            + " and/or attribute "
                            + request.getAttribute()
                            + " not found"));

        logStatus("get", request, 200, "OK");
        responseObserver.onNext(
            Rpc.GetResponse.newBuilder()
                .setStatus(200)
                .setValue(ByteString.copyFrom(kv.getValue()))
                .build());
      }
    } catch (Status s) {
      logStatus("get", request, s.statusCode, s.message);
      responseObserver.onNext(
          Rpc.GetResponse.newBuilder().setStatus(s.statusCode).setStatusMessage(s.message).build());
    } catch (TransactionRejectedException ex) {
      int status = ex.getResponseFlags() == Flags.DUPLICATE ? 204 : 412;
      logStatus("get", request, status, ex.getMessage());
      responseObserver.onNext(
          Rpc.GetResponse.newBuilder().setStatus(status).setStatusMessage(ex.getMessage()).build());
    } catch (Exception ex) {
      log.error("Failed to process request {}", request, ex);
      logStatus("get", request, 500, ex.getMessage());
      responseObserver.onNext(
          Rpc.GetResponse.newBuilder().setStatus(500).setStatusMessage(ex.getMessage()).build());
    }
    responseObserver.onCompleted();
  }

  @Override
  public void multifetch(
      MultifetchRequest request, StreamObserver<MultifetchResponse> responseObserver) {
    AtomicBoolean wasError = new AtomicBoolean();
    MultifetchResponse.Builder response = MultifetchResponse.newBuilder();
    for (GetRequest getRequest : request.getGetRequestList()) {
      get(
          getRequest.toBuilder().setTransactionId(request.getTransactionId()).build(),
          new StreamObserver<GetResponse>() {
            @Override
            public void onNext(GetResponse getResponse) {
              response.addGetResponse(getResponse);
            }

            @Override
            public void onError(Throwable throwable) {
              wasError.set(true);
              responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {}
          });
      if (wasError.get()) {
        break;
      }
    }
    if (!wasError.get()) {
      for (ListRequest listRequest : request.getListRequestList()) {
        listAttributes(
            listRequest.toBuilder().setTransactionId(request.getTransactionId()).build(),
            new StreamObserver<ListResponse>() {
              @Override
              public void onNext(ListResponse listResponse) {
                response.addListResponse(listResponse);
              }

              @Override
              public void onError(Throwable throwable) {
                wasError.set(true);
                responseObserver.onError(throwable);
              }

              @Override
              public void onCompleted() {}
            });
      }
      if (!wasError.get()) {
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
      }
    }
  }

  @Override
  public void scan(ScanRequest request, StreamObserver<ScanResult> responseObserver) {
    BlockingQueue<Optional<Throwable>> result = new SynchronousQueue<>();
    try {
      EntityDescriptor entity = repo.getEntity(request.getEntity());
      List<AttributeDescriptor<?>> attributes =
          request.getAttributeList().stream()
              .map(entity::getAttribute)
              .collect(Collectors.toList());
      List<DirectAttributeFamilyDescriptor> families =
          attributes.stream()
              .map(
                  a ->
                      Pair.of(
                          a,
                          direct.getFamiliesForAttribute(a).stream()
                              .filter(f -> f.getDesc().getAccess().canReadBatchSnapshot())
                              .findAny()))
              .map(
                  p ->
                      p.getSecond()
                          .orElseThrow(
                              () -> {
                                throw new IllegalArgumentException(
                                    "Missing batch-snapshot family for attribute " + p.getFirst());
                              }))
              .distinct()
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          families.size() == 1,
          "Got multiple families %s for attributes %s",
          families,
          request.getAttributeList());
      DirectAttributeFamilyDescriptor family = Iterables.getOnlyElement(families);
      BatchLogReader reader = Optionals.get(family.getBatchReader());
      ScanResult.Builder builder = ScanResult.newBuilder();
      AtomicInteger serializedEstimate = new AtomicInteger();
      reader.observe(
          reader.getPartitions(),
          attributes,
          new BatchLogObserver() {
            @Override
            public boolean onNext(StreamElement element) {
              if (!element.isDelete()) {
                builder.addValue(
                    Rpc.KeyValue.newBuilder()
                        .setAttribute(element.getAttribute())
                        .setKey(element.getKey())
                        .setValue(ByteString.copyFrom(element.getValue()))
                        .setStamp(element.getStamp()));
                int current =
                    serializedEstimate.addAndGet(
                        element.getValue().length
                            + element.getAttribute().length()
                            + element.getKey().length()
                            + 8);
                if (current > 65535) {
                  responseObserver.onNext(builder.build());
                  builder.clear();
                  serializedEstimate.set(0);
                }
              }
              return true;
            }

            @Override
            public void onCompleted() {
              if (builder.getValueCount() > 0) {
                responseObserver.onNext(builder.build());
              }
              ExceptionUtils.unchecked(() -> result.put(Optional.empty()));
            }

            @Override
            public boolean onError(Throwable error) {
              ExceptionUtils.unchecked(() -> result.put(Optional.of(error)));
              return false;
            }
          });
      Optional<Throwable> taken = ExceptionUtils.uncheckedFactory(result::take);
      if (taken.isPresent()) {
        Throwable err = taken.get();
        responseObserver.onError(
            io.grpc.Status.fromCode(Code.INTERNAL)
                .withCause(err)
                .withDescription(err.getMessage())
                .asRuntimeException());
      } else {
        responseObserver.onCompleted();
      }
    } catch (Exception ex) {
      responseObserver.onError(
          io.grpc.Status.fromCode(Code.INVALID_ARGUMENT)
              .withCause(ex)
              .withDescription(ex.getMessage())
              .asRuntimeException());
    }
  }

  private void noticeGetResult(
      GetRequest getRequest,
      EntityDescriptor entity,
      AttributeDescriptor<Object> attribute,
      Optional<KeyValue<Object>> maybeValue)
      throws TransactionRejectedException {

    if (!getRequest.getTransactionId().isEmpty()) {
      final KeyAttribute ka;
      if (maybeValue.isPresent()) {
        ka = KeyAttributes.ofStreamElement(maybeValue.get());
      } else if (attribute.isWildcard()) {
        ka =
            KeyAttributes.ofMissingAttribute(
                entity,
                getRequest.getKey(),
                attribute,
                getRequest.getAttribute().substring(attribute.toAttributePrefix().length()));
      } else {
        ka = KeyAttributes.ofMissingAttribute(entity, getRequest.getKey(), attribute);
      }
      updateTransaction(getRequest.getTransactionId(), Collections.singletonList(ka));
    }
  }

  @VisibleForTesting
  void updateTransaction(String transactionId, List<KeyAttribute> keyAttributes)
      throws TransactionRejectedException {

    transactionContext.get(transactionId).update(keyAttributes);
  }

  private void noticeListResult(
      ListRequest listRequest,
      EntityDescriptor entity,
      AttributeDescriptor<Object> attribute,
      Collection<KeyValue<Object>> values)
      throws TransactionRejectedException {

    if (!listRequest.getTransactionId().isEmpty()) {
      final List<KeyAttribute> kas =
          KeyAttributes.ofWildcardQueryElements(entity, listRequest.getKey(), attribute, values);
      updateTransaction(listRequest.getTransactionId(), kas);
    }
  }

  private RandomAccessReader instantiateReader(AttributeDescriptor<?> attr) throws Status {

    synchronized (readerMap) {
      RandomAccessReader reader = readerMap.get(attr);
      if (reader == null) {
        DirectAttributeFamilyDescriptor family =
            direct.getFamiliesForAttribute(attr).stream()
                .filter(af -> af.getDesc().getAccess().canRandomRead())
                .findAny()
                .orElseThrow(
                    () -> new Status(400, "Attribute " + attr + " has no random access family"));

        RandomAccessReader newReader =
            family
                .getRandomAccessReader()
                .orElseThrow(
                    () -> new Status(500, "Random access family " + family + " has no reader"));
        family.getAttributes().forEach(a -> readerMap.put(a, newReader));
        return newReader;
      }
      return reader;
    }
  }

  private static void logStatus(String name, MessageOrBuilder request, int status, String message) {
    log.info("{} {}: {} {}", name, TextFormat.shortDebugString(request), status, message);
  }
}
