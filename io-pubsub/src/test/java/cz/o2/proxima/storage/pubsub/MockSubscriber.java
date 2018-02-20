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

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import static org.mockito.Mockito.*;

/**
 * Mock {@link Subscriber}.
 */
public class MockSubscriber {

  public static Subscriber create(
      SubscriptionName subscription,
      MessageReceiver receiver,
      Supplier<PubsubMessage> supplier,
      ExecutorService executor) {

    Objects.requireNonNull(subscription);
    Objects.requireNonNull(receiver);
    Objects.requireNonNull(supplier);
    AtomicReference<Future<?>> f = new AtomicReference<>();
    Subscriber ret = mock(Subscriber.class);
    doAnswer(invocation -> {
      executor.submit(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          PubsubMessage msg = supplier.get();
          receiver.receiveMessage(msg, new AckReplyConsumer() {
            @Override
            public void ack() {
              // nop
            }

            @Override
            public void nack() {
              // nop
            }
          });
        }
      });
      return mock(ApiService.class);
    }).when(ret).startAsync();
    doAnswer(invocation -> {
      f.getAndUpdate(future -> {
        if (future != null) {
          future.cancel(true);
        }
        return null;
      });
      return null;
    }).when(ret).stopAsync();
    return ret;
  }

}
