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
package cz.o2.proxima.beam.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.beam.io.ProximaIO.WriteFn;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProximaIOWriteFnTest {

  private final Repository repository =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repository.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final WriteFn writeFn = new WriteFn(1000L, repository.asFactory());
  private RandomAccessReader reader;

  @Before
  public void setup() {
    writeFn.setUp();
    reader = Optionals.get(writeFn.getDirect().getRandomAccess(status));
  }

  @After
  public void tearDown() {
    writeFn.tearDown();
  }

  @Test
  public void writeSuccessfullyTest() {
    long now = System.currentTimeMillis();
    writeFn.startBundle();
    writeFn.processElement(
        StreamElement.upsert(
            gateway,
            status,
            UUID.randomUUID().toString(),
            "key1",
            status.getName(),
            now,
            new byte[] {1}));
    writeFn.finishBundle();
    Optional<KeyValue<byte[]>> keyValue = reader.get("key1", status, now);
    assertTrue(keyValue.isPresent());
  }

  @Test
  public void testTransactionRejection() {
    long now = System.currentTimeMillis();
    AtomicInteger fails = new AtomicInteger();
    AtomicInteger written = new AtomicInteger();
    OnlineAttributeWriter mockWriter = createSerializableWriter(fails, written);
    WriteFn modifiedWriteFn =
        new WriteFn(30000L, repository.asFactory()) {
          @Override
          OnlineAttributeWriter getWriterForElement(StreamElement element) {
            return mockWriter;
          }
        };
    modifiedWriteFn.startBundle();
    modifiedWriteFn.processElement(
        StreamElement.upsert(
            gateway,
            status,
            UUID.randomUUID().toString(),
            "key1",
            status.getName(),
            now,
            new byte[] {1}));
    assertEquals(1, fails.get());
    modifiedWriteFn.finishBundle();
    assertEquals(2, fails.get());
    assertEquals(1, written.get());
  }

  @NotNull
  private OnlineAttributeWriter createSerializableWriter(
      AtomicInteger fails, AtomicInteger written) {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    OnlineAttributeWriter mockWriter =
        new OnlineAttributeWriter() {
          @Override
          public void write(StreamElement data, CommitCallback statusCallback) {
            if (fails.incrementAndGet() < 2) {
              // reject after 1 second
              scheduler.schedule(
                  () ->
                      statusCallback.commit(
                          false, new TransactionRejectedException("t1", Response.Flags.ABORTED)),
                  1,
                  TimeUnit.SECONDS);
            } else {
              written.incrementAndGet();
              statusCallback.commit(true, null);
            }
          }

          @Override
          public Factory<? extends OnlineAttributeWriter> asFactory() {
            return repo -> this;
          }

          @Override
          public URI getUri() {
            return URI.create("mock:///");
          }

          @Override
          public void close() {}
        };
    return mockWriter;
  }
}
