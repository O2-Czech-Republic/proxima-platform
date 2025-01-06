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

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import cz.o2.proxima.io.pubsub.util.PubSubUtils;
import java.net.URI;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;

/** Test suite for {@link PubSubReader}. */
public class PubSubReaderTest extends AbstractPubSubReaderTest {

  public static class TestPubSubReader extends PubSubReader implements TestReader {

    private final Context context;
    @Getter private final Set<Integer> acked = new HashSet<>();
    @Getter private final Set<Integer> nacked = new HashSet<>();

    private Supplier<PubsubMessage> supplier;

    public TestPubSubReader(PubSubAccessor accessor, Context context) {
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

  private final PubSubAccessor accessor;

  public PubSubReaderTest() {
    Map<String, Object> cfg =
        ImmutableMap.<String, Object>builder()
            .put(
                "watermark.estimator-factory",
                PubSubReaderTest.TestWatermarkEstimatorFactory.class.getName())
            .build();
    this.accessor = new PubSubAccessor(storage, entity, URI.create("gps://my-project/topic"), cfg);
  }

  TestReader createReader() {
    return new TestPubSubReader(accessor, context);
  }

  @Override
  Deque<PubsubMessage> asMessages(StreamElement... elements) {
    return Arrays.stream(elements)
        .map(PubSubUtils::toKeyValue)
        .map(
            kv ->
                PubsubMessage.newBuilder()
                    .setMessageId(UUID.randomUUID().toString())
                    .setPublishTime(
                        Timestamp.newBuilder()
                            .setSeconds((int) (kv.getStamp() / 1000))
                            .setNanos((int) (kv.getStamp() % 1_000L) * 1_000_000))
                    .setData(kv.toByteString())
                    .build())
        .collect(Collectors.toCollection(LinkedList::new));
  }
}
