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
package cz.o2.proxima.client;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import cz.o2.proxima.proto.service.RetrieveServiceGrpc;
import cz.o2.proxima.proto.service.Rpc;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link IngestClient} for retrieving data. */
public class RetrieveServiceTest {

  private final String host = "localhost";
  private final int port = 4001;

  private final String serverName = "fake server for " + getClass().getName();

  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final IngestClient client = create(new Options());
  private Server fakeServer;

  @Before
  public void setup() throws IOException {
    fakeServer =
        InProcessServerBuilder.forName(serverName)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();
  }

  @After
  public void tearDown() {
    fakeServer.shutdownNow();
  }

  @Test
  public void testSynchronousValidGet() {
    mockRetrieveService(
        Collections.singletonList(Rpc.GetResponse.newBuilder().setStatus(200).build()));
    Rpc.GetResponse response =
        client.get(
            Rpc.GetRequest.newBuilder()
                .setEntity("gateway")
                .setKey("gw1")
                .setAttribute("armed")
                .build());
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testSimpleGet() {
    mockRetrieveService(
        Collections.singletonList(Rpc.GetResponse.newBuilder().setStatus(200).build()));
    Rpc.GetResponse response = client.get("gateway", "gw1", "armed");
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testSynchronousBadRequestGet() {
    mockRetrieveService(
        Collections.singletonList(Rpc.GetResponse.newBuilder().setStatus(400).build()));

    Rpc.GetResponse response =
        client.get(Rpc.GetRequest.newBuilder().setKey("gw1").setAttribute("armed").build());
    assertEquals(400, response.getStatus());
  }

  @Test
  public void testListAttributes() {
    mockRetrieveService(
        Collections.singletonList(
            Rpc.ListResponse.newBuilder()
                .setStatus(200)
                .addValue(
                    Rpc.ListResponse.AttrValue.newBuilder()
                        .setAttribute("armed")
                        .setValue(ByteString.EMPTY)
                        .build())
                .build()));
    Rpc.ListResponse response =
        client.listAttributes(
            Rpc.ListRequest.newBuilder().setEntity("gateway").setKey("gw1").build());
    assertEquals(200, response.getStatus());
    assertEquals(1, response.getValueCount());
    Rpc.ListResponse.AttrValue value = response.getValue(0);
    assertEquals("armed", value.getAttribute());
  }

  @Test
  public void testSimpleListAttributes() {
    mockRetrieveService(
        Collections.singletonList(
            Rpc.ListResponse.newBuilder()
                .setStatus(200)
                .addValue(
                    Rpc.ListResponse.AttrValue.newBuilder()
                        .setAttribute("armed")
                        .setValue(ByteString.EMPTY)
                        .build())
                .build()));
    Rpc.ListResponse response = client.listAttributes("gateway", "gw1");
    assertEquals(200, response.getStatus());
    assertEquals(1, response.getValueCount());
    Rpc.ListResponse.AttrValue value = response.getValue(0);
    assertEquals("armed", value.getAttribute());
  }

  @Test
  public void testListAttributesInvalidRequest() {
    mockRetrieveService(
        Collections.singletonList(Rpc.ListResponse.newBuilder().setStatus(400).build()));
    Rpc.ListResponse response =
        client.listAttributes(Rpc.ListRequest.newBuilder().setEntity("gateway").build());
    assertEquals(400, response.getStatus());
  }

  private void mockRetrieveService(List<?> responses) {
    RetrieveServiceGrpc.RetrieveServiceImplBase fakeRetrieveServiceIml =
        new RetrieveServiceGrpc.RetrieveServiceImplBase() {
          @Override
          public void get(
              Rpc.GetRequest request, StreamObserver<Rpc.GetResponse> responseObserver) {
            responses.forEach(r -> responseObserver.onNext((Rpc.GetResponse) r));
            responseObserver.onCompleted();
          }

          @Override
          public void listAttributes(
              Rpc.ListRequest request, StreamObserver<Rpc.ListResponse> responseObserver) {

            responses.forEach(r -> responseObserver.onNext((Rpc.ListResponse) r));
            responseObserver.onCompleted();
          }
        };
    serviceRegistry.addService(fakeRetrieveServiceIml);
  }

  private IngestClient create(Options opts) {
    return new IngestClient(host, port, opts) {

      @Override
      void createChannelAndStub() {
        this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        this.retrieveStub = RetrieveServiceGrpc.newBlockingStub(this.channel);
      }
    };
  }
}
