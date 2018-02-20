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
package cz.o2.proxima.storage.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorImpl;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import static cz.o2.proxima.storage.pubsub.Util.delete;
import static cz.o2.proxima.storage.pubsub.Util.deleteWildcard;
import static cz.o2.proxima.storage.pubsub.Util.update;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Test suite for {@link PubSubWriter}.
 */
public class PubSubWriterTest {

  private final Repository repo = Repository.of(ConfigFactory.load().resolve());
  private final AttributeDescriptorImpl<?> attr;
  private final AttributeDescriptorImpl<?> wildcard;
  private final EntityDescriptor entity;
  private final PubSubAccessor accessor;
  private TestPubSubWriter writer;

  private class TestPubSubWriter extends PubSubWriter {

    private final Context context;
    private Consumer<PubsubMessage> consumer;

    public TestPubSubWriter(Context context) {
      super(accessor, context);
      this.context = context;
    }

    void setConsumer(Consumer<PubsubMessage> consumer) {
      this.consumer = consumer;
    }

    @Override
    Publisher newPublisher(String project, String topic) throws IOException {
      return MockPublisher.create(project, topic, m -> {
        if (consumer != null) {
          consumer.accept(m);
        }
      });
    }

  }

  public PubSubWriterTest() throws URISyntaxException {
    this.attr = AttributeDescriptor.newBuilder(repo)
        .setEntity("entity")
        .setName("attr")
        .setSchemeURI(new URI("bytes:///"))
        .build();
    this.wildcard = AttributeDescriptor.newBuilder(repo)
        .setEntity("entity")
        .setName("wildcard.*")
        .setSchemeURI(new URI("bytes:///"))
        .build();
    this.entity = EntityDescriptor.newBuilder()
        .setName("entity")
        .addAttribute(attr)
        .addAttribute(wildcard)
        .build();
    assertTrue(entity.findAttribute("attr").isPresent());
    this.accessor = new PubSubAccessor(
        entity, new URI("gps://my-project/topic"), Collections.emptyMap());
  }

  @Before
  public void setUp() {
    Context context = new Context(() -> Executors.newCachedThreadPool()) { };
    try {
      writer = new TestPubSubWriter(context);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  @Test(timeout = 2000)
  public void testWrite() throws InterruptedException {
    long now = System.currentTimeMillis();
    List<PubsubMessage> written = new ArrayList<>();
    writer.setConsumer(written::add);
    CountDownLatch latch = new CountDownLatch(3);
    writer.write(PubSubReader.toElement(
        entity, update("key1", "attr", new byte[] { 1, 2 }, now)).get(),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    writer.write(PubSubReader.toElement(
        entity, delete("key2", "attr", now + 1000)).get(),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    writer.write(PubSubReader.toElement(
        entity, deleteWildcard("key3", wildcard, now)).get(),
        (succ, exc) -> {
          assertTrue(succ);
          latch.countDown();
        });
    latch.await();
    assertEquals(3, written.size());

    StreamElement elem = PubSubReader.toElement(entity, written.get(0)).get();
    assertEquals("key1", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertFalse(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertArrayEquals(new byte[] { 1, 2 }, elem.getValue());
    assertEquals(now, elem.getStamp());
    elem = PubSubReader.toElement(entity, written.get(1)).get();
    assertEquals("key2", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertEquals(now + 1000L, elem.getStamp());
    elem = PubSubReader.toElement(entity, written.get(2)).get();
    assertEquals("key3", elem.getKey());
    assertEquals(wildcard.toAttributePrefix() + "*", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertTrue(elem.isDeleteWildcard());
    assertEquals(now, elem.getStamp());
  }

  @Test(timeout = 2000)
  public void testWriteFail() throws InterruptedException {
    long now = System.currentTimeMillis();
    writer.setConsumer(e -> {
      throw new RuntimeException("Fail");
    });
    CountDownLatch latch = new CountDownLatch(3);
    writer.write(PubSubReader.toElement(
        entity, update("key1", "attr", new byte[] { 1, 2 }, now)).get(),
        (succ, exc) -> {
          assertFalse(succ);
          assertEquals("Fail", exc.getMessage());
          latch.countDown();
        });
    writer.write(PubSubReader.toElement(
        entity, delete("key2", "attr", now + 1000)).get(),
        (succ, exc) -> {
          assertFalse(succ);
          assertEquals("Fail", exc.getMessage());
          latch.countDown();
        });
    writer.write(PubSubReader.toElement(
        entity, deleteWildcard("key3", wildcard, now)).get(),
        (succ, exc) -> {
          assertFalse(succ);
          assertEquals("Fail", exc.getMessage());
          latch.countDown();
        });
    latch.await();
  }


}
