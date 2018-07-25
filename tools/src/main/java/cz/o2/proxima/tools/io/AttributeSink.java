/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.tools.io;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import cz.o2.proxima.client.IngestClient;
import cz.o2.proxima.proto.service.Rpc;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.util.Triple;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;

/**
 * Class for sinking data into specified attribute of entity.
 */
@Slf4j
public class AttributeSink implements DataSink<Triple<String, byte[], Long>> {

  private final String host;
  private final int port;
  private final EntityDescriptor entityDesc;
  private final AttributeDescriptor<?> desc;
  @Nullable
  private transient IngestClient client = null;

  public AttributeSink(
      String host,
      int port,
      EntityDescriptor entityDesc,
      AttributeDescriptor<?> desc) {

    this.host = host;
    this.port = port;
    this.entityDesc = entityDesc;
    this.desc = desc;
  }


  @Override
  public Writer<Triple<String, byte[], Long>> openWriter(int partitionId) {
    return new Writer<Triple<String, byte[], Long>>() {

      @Override
      public void write(Triple<String, byte[], Long> elem) throws IOException {
        CountDownLatch latch = new CountDownLatch(1);
        Rpc.Ingest ingest = Rpc.Ingest.newBuilder()
            .setEntity(entityDesc.getName())
            .setAttribute(desc.getName())
            .setKey(elem.getFirst())
            .setUuid(UUID.randomUUID().toString())
            .setValue(ByteString.copyFrom(elem.getSecond()))
            .setStamp(elem.getThird())
            .build();
        client().send(ingest, status -> {
          if (status.getStatus() != 200) {
            log.warn(
                "Failed to send ingest {}: {} {}",
                TextFormat.shortDebugString(ingest),
                status.getStatus(), status.getStatusMessage());
          }
          latch.countDown();
        });
      }

      @Override
      public void commit() throws IOException {
        // nop
      }

      @Override
      public void close() throws IOException {
        // nop
      }

    };
  }

  @Override
  public void commit() throws IOException {
    client.close();
  }

  @Override
  public void rollback() throws IOException {
    client.close();
  }

  private IngestClient client() {
    if (client == null) {
      client = IngestClient.create(host, port);
    }
    return client;
  }

}
