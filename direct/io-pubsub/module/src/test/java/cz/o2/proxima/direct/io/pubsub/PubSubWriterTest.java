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
package cz.o2.proxima.direct.io.pubsub;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.io.pubsub.util.PubSubUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/** Test suite for {@link PubSubWriter}. */
@Slf4j
public class PubSubWriterTest extends AbstractPubSubWriterTest {

  private static class TestPubSubWriter extends PubSubWriter implements TestWriter {

    private Consumer<PubsubMessage> consumer;

    public TestPubSubWriter(PubSubAccessor accessor, Context context) {
      super(accessor, context);
    }

    @Override
    public void setConsumer(Consumer<PubsubMessage> consumer) {
      this.consumer = consumer;
    }

    @Override
    Publisher newPublisher(String project, String topic) {
      return MockPublisher.create(
          project,
          topic,
          m -> {
            if (consumer != null) {
              consumer.accept(m);
            }
          });
    }
  }

  private final PubSubAccessor accessor;

  public PubSubWriterTest() {
    accessor =
        new PubSubAccessor(
            storage, entity, URI.create("gps://my-project/topic"), Collections.emptyMap());
  }

  @Test
  public void testAsFactorySerializable() throws IOException, ClassNotFoundException {
    byte[] bytes = TestUtils.serializeObject(writer.asFactory());
    AttributeWriterBase.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(accessor.getUri(), factory.apply(repo).getUri());
  }

  @Override
  public TestWriter createWriter() {
    return new TestPubSubWriter(accessor, context);
  }

  @Override
  void validateWrittenElements(List<PubsubMessage> written, long now) {
    assertEquals(3, written.size());

    StreamElement elem =
        Optionals.get(
            PubSubUtils.toStreamElement(
                entity, UUID.randomUUID().toString(), written.get(0).getData().toByteArray()));
    assertEquals("key1", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertFalse(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertArrayEquals(new byte[] {1, 2}, elem.getValue());
    assertEquals(now, elem.getStamp());
    elem =
        Optionals.get(
            PubSubUtils.toStreamElement(
                entity, UUID.randomUUID().toString(), written.get(1).getData().toByteArray()));
    assertEquals("key2", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertEquals(now + 1000L, elem.getStamp());
    elem =
        Optionals.get(
            PubSubUtils.toStreamElement(
                entity, UUID.randomUUID().toString(), written.get(2).getData().toByteArray()));
    assertEquals("key3", elem.getKey());
    assertEquals(wildcard.toAttributePrefix() + "*", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertTrue(elem.isDeleteWildcard());
    assertEquals(now, elem.getStamp());
  }
}
