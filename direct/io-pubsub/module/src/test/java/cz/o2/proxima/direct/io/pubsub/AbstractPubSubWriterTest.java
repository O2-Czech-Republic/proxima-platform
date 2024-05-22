/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.pubsub;

import static cz.o2.proxima.direct.io.pubsub.Util.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.io.pubsub.util.PubSubUtils;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public abstract class AbstractPubSubWriterTest {

  interface TestWriter extends OnlineAttributeWriter {

    void setConsumer(Consumer<PubsubMessage> consumer);
  }

  protected final Repository repo = Repository.ofTest(ConfigFactory.load().resolve());
  protected final AttributeDescriptor<?> attr;
  protected final AttributeDescriptor<?> wildcard;
  protected final EntityDescriptor entity;
  protected final PubSubStorage storage = new PubSubStorage();

  protected final DirectDataOperator direct =
      repo.getOrCreateOperator(
          DirectDataOperator.class,
          op ->
              op.withExecutorFactory(
                  () ->
                      Executors.newFixedThreadPool(
                          5,
                          runnable -> {
                            Thread t = new Thread(runnable);
                            t.setName(PubSubWriterTest.class.getSimpleName());
                            t.setDaemon(true);
                            t.setUncaughtExceptionHandler(
                                (thr, err) -> log.error("Error in thread {}", thr.getName(), err));
                            return t;
                          })));
  protected final Context context = direct.getContext();

  TestWriter writer;

  @Before
  public void setUp() {
    writer = createWriter();
  }

  @After
  public void tearDown() {
    writer.close();
  }

  abstract TestWriter createWriter();

  public AbstractPubSubWriterTest() {
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("attr")
            .setSchemeUri(URI.create("bytes:///"))
            .build();
    this.wildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("entity")
            .setName("wildcard.*")
            .setSchemeUri(URI.create("bytes:///"))
            .build();
    this.entity =
        EntityDescriptor.newBuilder()
            .setName("entity")
            .addAttribute(attr)
            .addAttribute(wildcard)
            .build();
  }

  @Test(timeout = 10000)
  public void testWrite() throws InterruptedException, IOException {
    long now = System.currentTimeMillis();
    List<PubsubMessage> written = new ArrayList<>();
    writer.setConsumer(written::add);
    CountDownLatch latch = new CountDownLatch(3);
    writer.write(
        Optionals.get(
            PubSubUtils.toStreamElement(
                entity,
                UUID.randomUUID().toString(),
                update("key1", "attr", new byte[] {1, 2}, now).getData().toByteArray())),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    writer.write(
        Optionals.get(
            PubSubUtils.toStreamElement(
                entity,
                UUID.randomUUID().toString(),
                delete("key2", "attr", now + 1000).getData().toByteArray())),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    writer.write(
        Optionals.get(
            PubSubUtils.toStreamElement(
                entity,
                UUID.randomUUID().toString(),
                deleteWildcard("key3", wildcard, now).getData().toByteArray())),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    validateWrittenElements(written, now);
  }

  abstract void validateWrittenElements(List<PubsubMessage> written, long now) throws IOException;

  @Test(timeout = 10000)
  public void testWriteFail() throws InterruptedException {
    long now = System.currentTimeMillis();
    writer.setConsumer(
        e -> {
          throw new RuntimeException("Fail");
        });
    CountDownLatch latch = new CountDownLatch(3);
    writer.write(
        Optionals.get(
            PubSubUtils.toStreamElement(
                entity,
                UUID.randomUUID().toString(),
                update("key1", "attr", new byte[] {1, 2}, now).getData().toByteArray())),
        (succ, exc) -> {
          assertFalse(succ);
          assertEquals("Fail", exc.getMessage());
          latch.countDown();
        });
    writer.write(
        Optionals.get(
            PubSubUtils.toStreamElement(
                entity,
                UUID.randomUUID().toString(),
                delete("key2", "attr", now + 1000).getData().toByteArray())),
        (succ, exc) -> {
          assertFalse(succ);
          assertEquals("Fail", exc.getMessage());
          latch.countDown();
        });
    writer.write(
        Optionals.get(
            PubSubUtils.toStreamElement(
                entity,
                UUID.randomUUID().toString(),
                deleteWildcard("key3", wildcard, now).getData().toByteArray())),
        (succ, exc) -> {
          assertFalse(succ);
          assertEquals("Fail", exc.getMessage());
          latch.countDown();
        });
    latch.await();
  }
}
