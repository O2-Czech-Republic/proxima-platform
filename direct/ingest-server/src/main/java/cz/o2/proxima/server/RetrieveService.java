/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.server.metrics.Metrics;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for reading data.
 */
@Slf4j
public class RetrieveService extends RetrieveServiceGrpc.RetrieveServiceImplBase {

  private final Map<AttributeDescriptor<?>, RandomAccessReader> readerMap;
  private final Repository repo;
  private final DirectDataOperator direct;

  public RetrieveService(Repository repo, DirectDataOperator direct) {
    this.repo = repo;
    this.direct = direct;
    this.readerMap = Collections.synchronizedMap(new HashMap<>());
  }

  private class Status extends Exception {
    final int statusCode;
    final String message;
    Status(int statusCode, String message) {
      this.statusCode = statusCode;
      this.message = message;
    }
  }

  @Override
  public void listAttributes(
      Rpc.ListRequest request,
      StreamObserver<Rpc.ListResponse> responseObserver) {

    try {
      Metrics.LIST_REQUESTS.increment();
      log.info("Processing listAttributes {}", TextFormat.shortDebugString(request));
      if (request.getEntity().isEmpty() || request.getKey().isEmpty()
          || request.getWildcardPrefix().isEmpty()) {
        throw new Status(400, "Missing some required fields");
      }

      EntityDescriptor entity = repo.findEntity(request.getEntity())
          .orElseThrow(() -> new Status(
              404, "Entity " + request.getEntity() + " not found"));

      AttributeDescriptor<Object> wildcard = entity.findAttribute(
          request.getWildcardPrefix() + ".*").orElseThrow(
              () -> new Status(404, "Entity " + request.getEntity()
                  + " does not have wildcard attribute "
                  + request.getWildcardPrefix()));

      RandomAccessReader reader = instantiateReader(wildcard);

      Rpc.ListResponse.Builder response = Rpc.ListResponse.newBuilder()
          .setStatus(200);

      synchronized (reader) {
        reader.scanWildcard(
            request.getKey(), wildcard,
            reader.fetchOffset(RandomAccessReader.Listing.ATTRIBUTE, request.getOffset()),
            request.getLimit() > 0 ? request.getLimit() : -1,
            kv -> response.addValue(
                Rpc.ListResponse.AttrValue.newBuilder()
                    .setAttribute(kv.getAttribute())
                    .setValue(ByteString.copyFrom(kv.getValueBytes()))));
      }
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    } catch (Status s) {
      responseObserver.onNext(Rpc.ListResponse.newBuilder()
            .setStatus(s.statusCode)
            .setStatusMessage(s.message)
            .build());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      log.error("Failed to process request {}", request, ex);
      responseObserver.onNext(Rpc.ListResponse.newBuilder()
          .setStatus(500)
          .setStatusMessage(ex.getMessage())
          .build());
      responseObserver.onCompleted();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void get(
      Rpc.GetRequest request,
      StreamObserver<Rpc.GetResponse> responseObserver) {

    Metrics.GET_REQUESTS.increment();
    log.info("Processing get {}", TextFormat.shortDebugString(request));
    try {
      if (request.getEntity().isEmpty() || request.getKey().isEmpty()
          || request.getAttribute().isEmpty()) {
        throw new Status(400, "Missing some required fields");
      }

      EntityDescriptor entity = repo.findEntity(request.getEntity())
          .orElseThrow(() -> new Status(
              404, "Entity " + request.getEntity() + " not found"));

      AttributeDescriptor<Object> attribute = entity.findAttribute(
          request.getAttribute()).orElseThrow(
              () -> new Status(404, "Entity " + request.getEntity()
                  + " does not have attribute "
                  + request.getAttribute()));

      RandomAccessReader reader = instantiateReader(attribute);

      synchronized (reader) {
        KeyValue<Object> kv = reader
            .get(request.getKey(), request.getAttribute(), attribute)
            .orElseThrow(() -> new Status(
                404,
                "Key " + request.getKey() + " and/or attribute "
                    + request.getAttribute() + " not found"));

        responseObserver.onNext(Rpc.GetResponse.newBuilder()
            .setStatus(200)
            .setValue(ByteString.copyFrom(kv.getValueBytes()))
            .build());
      }

      responseObserver.onCompleted();
    } catch (Status s) {
      responseObserver.onNext(Rpc.GetResponse.newBuilder()
            .setStatus(s.statusCode)
            .setStatusMessage(s.message)
            .build());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      log.error("Failed to process request {}", request, ex);
      responseObserver.onNext(Rpc.GetResponse.newBuilder()
          .setStatus(500)
          .setStatusMessage(ex.getMessage())
          .build());
      responseObserver.onCompleted();
    }

  }

  private RandomAccessReader instantiateReader(
      AttributeDescriptor<?> attr) throws Status {

    synchronized (readerMap) {

      RandomAccessReader reader = readerMap.get(attr);
      if (reader == null) {
        DirectAttributeFamilyDescriptor family = direct.getFamiliesForAttribute(attr)
            .stream()
            .filter(af -> af.getDesc().getAccess().canRandomRead())
            .findAny()
            .orElseThrow(() -> new Status(400, "Attribute " +  attr
                + " has no random access family"));

        RandomAccessReader newReader = family
            .getRandomAccessReader()
            .orElseThrow(() -> new Status(500, "Random access family "
                + family + " has no reader"));
        family.getAttributes().forEach(a -> readerMap.put(a, newReader));
        return newReader;
      }
      return reader;

    }

  }

}

