/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.TextFormat;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.proto.service.Rpc.GetRequest;
import cz.o2.proxima.proto.service.Rpc.GetResponse;
import cz.o2.proxima.proto.service.Rpc.ListRequest;
import cz.o2.proxima.proto.service.Rpc.ListResponse;
import cz.o2.proxima.proto.service.Rpc.MultifetchRequest;
import cz.o2.proxima.proto.service.Rpc.MultifetchResponse;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.server.transaction.TransactionContext;
import cz.o2.proxima.transaction.KeyAttribute;
import cz.o2.proxima.transaction.KeyAttributes;
import cz.o2.proxima.transaction.Response.Flags;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
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

      List<KeyValue<Object>> kvs = new ArrayList<>();
      Rpc.ListResponse.Builder response = Rpc.ListResponse.newBuilder().setStatus(200);

      synchronized (reader) {
        reader.scanWildcard(
            request.getKey(),
            wildcard,
            reader.fetchOffset(RandomAccessReader.Listing.ATTRIBUTE, request.getOffset()),
            request.getLimit() > 0 ? request.getLimit() : -1,
            kvs::add);
        kvs.forEach(
            kv ->
                response.addValue(
                    Rpc.ListResponse.AttrValue.newBuilder()
                        .setAttribute(kv.getAttribute())
                        .setValue(ByteString.copyFrom(kv.getValue()))));
      }
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
