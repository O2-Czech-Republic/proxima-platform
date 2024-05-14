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

import static cz.o2.proxima.direct.io.pubsub.PubSubBulkReader.inflate;
import static org.junit.Assert.*;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.io.pubsub.proto.PubSub.Bulk;
import cz.o2.proxima.io.pubsub.proto.PubSub.BulkWrapper;
import cz.o2.proxima.io.pubsub.proto.PubSub.BulkWrapper.Compression;
import cz.o2.proxima.io.pubsub.util.PubSubUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PubSubBulkWriterTest extends AbstractPubSubWriterTest {

  private static class TestPubSubBulkWriter extends PubSubBulkWriter implements TestWriter {

    private Consumer<PubsubMessage> consumer;

    public TestPubSubBulkWriter(PubSubAccessor accessor, Context context) {
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

  @Parameters
  public static List<Boolean> params() {
    return Arrays.asList(false, true);
  }

  @Parameter public boolean deflate;

  private PubSubAccessor accessor;

  @Before
  public void setUp() {
    accessor = new PubSubAccessor(storage, entity, getUri(), Collections.emptyMap());
    super.setUp();
  }

  private URI getUri() {
    return deflate
        ? URI.create("gps-bulk://my-project/topic?deflate=true")
        : URI.create("gps-bulk://my-project/topic");
  }

  @Override
  TestWriter createWriter() {
    return new TestPubSubBulkWriter(accessor, context);
  }

  @Override
  void validateWrittenElements(List<PubsubMessage> written, long now) throws IOException {
    assertEquals(1, written.size());
    Bulk bulk = Bulk.parseFrom(maybeInflate(written.get(0).getData().toByteArray()));
    assertEquals(3, bulk.getKvCount());
    assertEquals(3, bulk.getUuidCount());
    StreamElement elem =
        Optionals.get(PubSubUtils.toStreamElement(entity, bulk.getUuid(0), bulk.getKv(0)));
    assertEquals("key1", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertFalse(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertArrayEquals(new byte[] {1, 2}, elem.getValue());
    assertEquals(now, elem.getStamp());
    elem = Optionals.get(PubSubUtils.toStreamElement(entity, bulk.getUuid(1), bulk.getKv(1)));
    assertEquals("key2", elem.getKey());
    assertEquals("attr", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertFalse(elem.isDeleteWildcard());
    assertEquals(now + 1000L, elem.getStamp());
    elem = Optionals.get(PubSubUtils.toStreamElement(entity, bulk.getUuid(2), bulk.getKv(2)));
    assertEquals("key3", elem.getKey());
    assertEquals(wildcard.toAttributePrefix() + "*", elem.getAttribute());
    assertTrue(elem.isDelete());
    assertTrue(elem.isDeleteWildcard());
    assertEquals(now, elem.getStamp());
  }

  private byte[] maybeInflate(byte[] bytes) throws IOException {
    BulkWrapper wrapper = BulkWrapper.parseFrom(bytes);
    assertEquals(deflate ? Compression.DEFLATE : Compression.NONE, wrapper.getCompression());
    if (deflate) {
      return inflate(wrapper.getValue().toByteArray());
    }
    return wrapper.getValue().toByteArray();
  }
}
