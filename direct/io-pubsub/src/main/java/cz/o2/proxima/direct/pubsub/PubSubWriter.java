/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.core.AbstractOnlineAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.pubsub.proto.PubSub;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/** A {@link OnlineAttributeWriter} for Google PubSub. */
@Stable
@Slf4j
class PubSubWriter extends AbstractOnlineAttributeWriter implements OnlineAttributeWriter {

  private final PubSubAccessor accessor;
  private final Context context;
  private final AtomicInteger inflight = new AtomicInteger();
  private final Serializable flightLock = new Serializable() {};
  private volatile boolean closed = false;
  private transient boolean initialized = false;
  private transient Publisher publisher;
  private transient ExecutorService executor;

  PubSubWriter(PubSubAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getUri());
    this.accessor = accessor;
    this.context = context;
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
        throw new RuntimeException(ex);
      }
    }
  }

  @VisibleForTesting
  Publisher newPublisher(String project, String topic) throws IOException {
    return Publisher.newBuilder(ProjectTopicName.of(project, topic)).build();
  }

  @Override
  public synchronized void write(StreamElement data, CommitCallback statusCallback) {

    initialize();
    log.debug("Writing data {} to {}", data, getUri());
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
                  .setMessageId(data.getUuid())
                  .setPublishTime(
                      Timestamp.newBuilder()
                          .setSeconds(data.getStamp() / 1000)
                          .setNanos((int) (data.getStamp() % 1000) * 1_000_000))
                  .setData(
                      PubSub.KeyValue.newBuilder()
                          .setKey(data.getKey())
                          .setAttribute(data.getAttribute())
                          .setDelete(data.isDelete())
                          .setDeleteWildcard(data.isDeleteWildcard())
                          .setValue(
                              data.isDelete()
                                  ? ByteString.EMPTY
                                  : ByteString.copyFrom(data.getValue()))
                          .setStamp(data.getStamp())
                          .build()
                          .toByteString())
                  .build());

      ApiFutures.addCallback(
          future,
          new ApiFutureCallback<String>() {

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
              log.warn("Failed to publish element {} to pubsub", data, thrwbl);
              handle(false, thrwbl);
            }

            @Override
            public void onSuccess(String v) {
              log.debug("Committing processing of {} with success", data);
              handle(true, null);
            }
          },
          executor);
    } catch (Throwable err) {
      log.warn("Failed to publish {} to pubsub", data, err);
      statusCallback.commit(false, err);
    }
  }

  @Override
  public OnlineAttributeWriter.Factory<?> asFactory() {
    final PubSubAccessor accessor = this.accessor;
    final Context context = this.context;
    return repo -> new PubSubWriter(accessor, context);
  }

  @Override
  public synchronized void close() {
    if (publisher != null) {
      try {
        closed = true;
        while (inflight.get() != 0) {
          synchronized (flightLock) {
            flightLock.wait(100);
          }
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        publisher.shutdown();
      } catch (Exception ex) {
        log.warn("Failed to shutdown publisher {}", publisher, ex);
      }
      publisher = null;
      initialized = false;
    }
  }
}
