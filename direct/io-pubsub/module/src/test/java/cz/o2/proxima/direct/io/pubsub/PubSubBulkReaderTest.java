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

import static cz.o2.proxima.direct.io.pubsub.PubSubBulkWriter.deflate;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import cz.o2.proxima.io.pubsub.proto.PubSub.Bulk;
import cz.o2.proxima.io.pubsub.proto.PubSub.BulkWrapper;
import cz.o2.proxima.io.pubsub.proto.PubSub.BulkWrapper.Compression;
import cz.o2.proxima.io.pubsub.util.PubSubUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PubSubBulkReaderTest extends AbstractPubSubReaderTest {

  public static class TestPubSubBulkReader extends PubSubBulkReader implements TestReader {

    private final Context context;
    @Getter private final Set<Integer> acked = new HashSet<>();
    @Getter private final Set<Integer> nacked = new HashSet<>();

    private Supplier<PubsubMessage> supplier;

    public TestPubSubBulkReader(PubSubAccessor accessor, Context context) {
      super(accessor, context);
      this.context = context;
    }

    @Override
    public void setSupplier(Supplier<PubsubMessage> supplier) {
      this.supplier = supplier;
    }

    @Override
    Subscriber newSubscriber(ProjectSubscriptionName subscription, MessageReceiver receiver) {

      return MockSubscriber.create(
          subscription, receiver, supplier, acked, nacked, context.getExecutorService());
    }
  }

  @Parameters
  public static List<Boolean> parameters() {
    return Arrays.asList(false, true);
  }

  @Parameter public boolean deflate;

  private PubSubAccessor accessor;

  @Before
  public void setUp() {
    Map<String, Object> cfg =
        ImmutableMap.<String, Object>builder()
            .put(
                "watermark.estimator-factory",
                PubSubReaderTest.TestWatermarkEstimatorFactory.class.getName())
            .build();
    accessor = new PubSubAccessor(storage, entity, getUri(), cfg);
    Preconditions.checkState(accessor.getBulk().isDeflate() == deflate);
    super.setUp();
  }

  private URI getUri() {
    return deflate
        ? URI.create("gps-bulk://my-project/topic?deflate=true")
        : URI.create("gps-bulk://my-project/topic");
  }

  TestReader createReader() {
    return new TestPubSubBulkReader(accessor, context);
  }

  @Override
  Deque<PubsubMessage> asMessages(StreamElement... elements) throws IOException {
    Bulk bulk =
        Bulk.newBuilder()
            .addAllKv(
                Arrays.stream(elements).map(PubSubUtils::toKeyValue).collect(Collectors.toList()))
            .addAllUuid(
                Arrays.stream(elements).map(StreamElement::getUuid).collect(Collectors.toList()))
            .build();
    BulkWrapper wrapper =
        BulkWrapper.newBuilder()
            .setCompression(deflate ? Compression.DEFLATE : Compression.NONE)
            .setValue(ByteString.copyFrom(maybeDeflate(bulk.toByteArray())))
            .build();
    long now = System.currentTimeMillis();
    return Stream.of(
            PubsubMessage.newBuilder()
                .setMessageId(UUID.randomUUID().toString())
                .setPublishTime(
                    Timestamp.newBuilder()
                        .setSeconds((int) (now / 1000))
                        .setNanos((int) (now % 1_000L) * 1_000_000))
                .setData(wrapper.toByteString())
                .build())
        .collect(Collectors.toCollection(LinkedList::new));
  }

  private byte[] maybeDeflate(byte[] data) throws IOException {
    return deflate ? deflate(data) : data;
  }
}
