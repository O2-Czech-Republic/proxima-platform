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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AbstractPubSubWriter {
  protected final PubSubAccessor accessor;
  protected final Context context;
  private final AtomicInteger inflight = new AtomicInteger();
  private final Object flightLock = new Object();
  private volatile boolean closed = false;
  private boolean initialized = false;
  private @Nullable Publisher publisher;
  private @Nullable ExecutorService executor;

  public AbstractPubSubWriter(PubSubAccessor accessor, Context context) {
    this.accessor = accessor;
    this.context = context;
  }

  public URI getUri() {
    return accessor.getUri();
  }

  synchronized void initialize() {
    if (!initialized) {
      try {
        publisher = newPublisher(accessor.getProject(), accessor.getTopic());
        executor = context.getExecutorService();
        initialized = true;
        closed = false;
      } catch (IOException ex) {
        if (publisher != null) {
          ExceptionUtils.unchecked(() -> publisher.shutdown());
        }
        throw new IllegalStateException(ex);
      }
    }
  }

  @VisibleForTesting
  Publisher newPublisher(String project, String topic) throws IOException {
    return Publisher.newBuilder(ProjectTopicName.of(project, topic)).build();
  }

  protected synchronized void writeMessage(
      String messageId, long stamp, Message msg, CommitCallback statusCallback) {

    initialize();
    try {
      if (inflight.incrementAndGet() >= 1000) {
        while (inflight.get() >= 1000) {
          synchronized (flightLock) {
            flightLock.wait(1000);
          }
        }
      }
      ApiFuture<String> future =
          publisher.publish(
              PubsubMessage.newBuilder()
                  .setMessageId(messageId)
                  .setPublishTime(
                      Timestamp.newBuilder()
                          .setSeconds(stamp / 1000)
                          .setNanos((int) (stamp % 1000) * 1_000_000))
                  .setData(msg.toByteString())
                  .build());

      ApiFutures.addCallback(
          future,
          new ApiFutureCallback<>() {

            private void handle(boolean success, Throwable thrwbl) {
              statusCallback.commit(success, thrwbl);
              if (inflight.getAndDecrement() >= 1000 || closed) {
                synchronized (flightLock) {
                  flightLock.notifyAll();
                }
              }
            }

            @Override
            public void onFailure(Throwable thrwbl) {
              handle(false, thrwbl);
            }

            @Override
            public void onSuccess(String v) {
              handle(true, null);
            }
          },
          executor);
    } catch (Throwable err) {
      if (err instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      statusCallback.commit(false, err);
    }
  }

  public synchronized void close() {
    if (publisher != null) {
      try {
        closed = true;
        while (inflight.get() > 0) {
          synchronized (flightLock) {
            flightLock.wait(100);
          }
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        publisher.shutdown();
      } catch (Exception ex) {
        log.warn("Failed to shutdown publisher {}", publisher, ex);
        if (ex instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
      publisher = null;
      initialized = false;
    }
  }
}
