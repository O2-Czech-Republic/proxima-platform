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

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.pubsub.proto.PubSub;
import cz.seznam.euphoria.core.annotation.stability.Experimental;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.threeten.bp.Duration;

/**
 * A {@link CommitLogReader} for Google PubSub.
 */
@Experimental
@Slf4j
class PubSubReader extends AbstractStorage implements CommitLogReader {

  private static class PubSubOffset implements Offset {
    @Override
    public Partition getPartition() {
      return () -> 0;
    }
  }

  private final Context context;
  private final String project;
  private final String topic;
  private final PubSubOffset offset = new PubSubOffset();
  private final int maxAckDeadline;
  private final int subscriptionAckDeadline;
  private final boolean subscriptionAutoCreate;

  private transient ExecutorService executor;

  PubSubReader(PubSubAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getURI());
    this.context = context;
    this.project = accessor.getProject();
    this.topic = accessor.getTopic();
    this.maxAckDeadline = accessor.getMaxAckDeadline();
    this.subscriptionAckDeadline = accessor.getSubscriptionAckDeadline();
    this.subscriptionAutoCreate = accessor.isSubscriptionAutoCreate();
  }

  @Override
  public List<Partition> getPartitions() {
    // pubsub has only single partition from the client perspective
    return Arrays.asList(() -> 0);
  }

  @Override
  public ObserveHandle observe(
      @Nullable String name, Position position, LogObserver observer) {

    validatePosition(position);
    return consume(name, (e, c) -> {
      LogObserver.OffsetCommitter committer = (succ, exc) -> {
        if (succ) {
          log.debug("Confirming message {} to PubSub", e);
          c.ack();
        } else {
          if (exc != null) {
            log.warn("Error during processing of {}", e, exc);
          } else {
            log.info("Nacking message {} by request", e);
          }
          c.nack();
        }
      };
      try {
        boolean ret = observer.onNext(e, committer);
        if (!ret) {
          observer.onCompleted();
        }
        return ret;
      } catch (Exception ex) {
        log.error("Error calling onNext", ex);
        committer.fail(ex);
        throw new RuntimeException(ex);
      }
    }, observer::onError, null, () -> { }, observer::onCancelled);
  }

  @Override
  public ObserveHandle observePartitions(
      @Nullable String name, Collection<Partition> partitions, Position position,
      boolean stopAtCurrent, LogObserver observer) {

    if (stopAtCurrent) {
      throw new UnsupportedOperationException(
          "PubSub can observe only current data.");
    }
    return observe(name, position, observer);
  }

  /**
   * Observe PubSub in a bulk fashion.
   * Note that due to current PubSub implementation the bulk commit must
   * happen before the ack timeout. If the message is not acknowledged before this
   * timeout the message will be redelivered, which will result in duplicate
   * messages.
   * @param name name of the observer subscription
   * @param position must be set to NEWEST
   * @param stopAtCurrent throw {@link UnsupportedOperationException} when {@code true}
   * @param observer the observer of data
   * @return handle to interact with the observation thread
   */
  @Override
  public ObserveHandle observeBulk(
      @Nullable String name, Position position,
      boolean stopAtCurrent,
      BulkLogObserver observer) {

    if (stopAtCurrent) {
      throw new UnsupportedOperationException(
          "PubSub can observe only current data.");
    }

    validatePosition(position);
    AtomicReference<List<AckReplyConsumer>> unconfirmed = new AtomicReference<>(
        new ArrayList<>());
    Object lock = new Object();
    Object listLock = new Object();
    AtomicLong globalOffset = new AtomicLong();
    return consume(name, (e, c) -> {
      AtomicLong confirmUntil = new AtomicLong();
      synchronized (listLock) {
        List<AckReplyConsumer> list = unconfirmed.get();
        list.add(c);
        confirmUntil.set(list.size() + globalOffset.get());
      }
      BulkLogObserver.OffsetCommitter committer = (succ, exc) -> {
        // the implementation can use some other
        // thread for this, so we need to synchronize this
        synchronized (listLock) {
          int confirmCount = (int) (confirmUntil.get() - globalOffset.get());
          if (confirmCount > 0) {
            final Consumer<AckReplyConsumer> apply;
            if (succ) {
              log.debug("Bulk confirming {} messages", confirmCount);
              apply = AckReplyConsumer::ack;
            } else {
              if (exc != null) {
                log.warn("Error during processing of last bulk", exc);
              } else {
                log.info("Nacking last bulk by request");
              }
              apply = AckReplyConsumer::nack;
            }
            List<AckReplyConsumer> list = unconfirmed.get();
            for (int i = 0; i < confirmCount; i++) {
                apply.accept(list.get(i));
            }
            globalOffset.addAndGet(confirmCount);
            unconfirmed.set(Lists.newArrayList(list.subList(confirmCount, list.size())));
          }
        }
      };

      // our observers are not supposed to be thread safe, so we must
      // ensure explicit synchronization here
      synchronized (lock) {
        try {
          if (!observer.onNext(e, () -> 0, committer)) {
            observer.onCompleted();
            return false;
          }
          return true;
        } catch (Exception ex) {
          log.error("Error calling on next", ex);
          committer.fail(ex);
          throw new RuntimeException(ex);
        }
      }
    }, observer::onError,
    () -> observer.onRestart(Arrays.asList(() -> () -> 0)),
    () -> observer.onRestart(Arrays.asList(() -> () -> 0)),
    observer::onCancelled);
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      @Nullable String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      BulkLogObserver observer) {

    if (stopAtCurrent) {
      throw new UnsupportedOperationException(
          "PubSub can observe only current data.");
    }

    return observeBulk(name, position, observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(
      Collection<Offset> offsets, BulkLogObserver observer) {

    return observeBulkPartitions(
        offsets.stream().map(Offset::getPartition).collect(Collectors.toList()),
        Position.NEWEST, observer);
  }

  @Override
  public void close() throws IOException {
    // nop
  }

  @VisibleForTesting
  Subscriber newSubscriber(ProjectSubscriptionName subscription, MessageReceiver receiver) {
    if (subscriptionAutoCreate) {
      try (SubscriptionAdminClient client = SubscriptionAdminClient.create()) {
        try {
          client.createSubscription(
              subscription, ProjectTopicName.of(project, topic),
              PushConfig.newBuilder().build(), this.subscriptionAckDeadline);
          log.info(
              "Automatically creating subscription {} for topic {} as requested",
              subscription, topic);
        } catch (AlreadyExistsException ex) {
          log.debug("Subscription {} already exists. Skipping creation.", subscription);
        }
      } catch (IOException ex) {
        log.error("Failed to close SubscriptionAdminClient", ex);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return Subscriber.newBuilder(subscription, receiver)
        .setMaxAckExtensionPeriod(Duration.ofMillis(maxAckDeadline))
        .build();
  }

  private void validatePosition(Position position) throws UnsupportedOperationException {
    if (position == Position.OLDEST) {
      throw new UnsupportedOperationException("PubSub can observe only current data.");
    }
  }

  private ObserveHandle consume(
      @Nullable String name,
      BiFunction<StreamElement, AckReplyConsumer, Boolean> consumer,
      Function<Throwable, Boolean> errorHandler,
      @Nullable Runnable onInit,
      Runnable onRestart,
      Runnable onCancel) {

    name = name != null ? name : "unnamed-consumer-" + UUID.randomUUID().toString();
    ProjectSubscriptionName subscription = ProjectSubscriptionName.of(project, name);
    AtomicReference<Subscriber> subscriber = new AtomicReference<>();
    AtomicReference<MessageReceiver> receiver = new AtomicReference<>();
    AtomicBoolean stopProcessing = new AtomicBoolean();
    receiver.set((m, c) -> {
      try {
        if (stopProcessing.get()) {
          log.debug("Returning rejected message {}", m);
          c.nack();
          return;
        }
        Optional<StreamElement> elem = toElement(getEntityDescriptor(), m);
        if (elem.isPresent()) {
          if (!consumer.apply(elem.get(), c)) {
            log.info("Terminating consumption by request.");
            stopAsync(subscriber);
          }
        } else {
          log.warn("Skipping unparseable element {}", m);
          c.ack();
        }
      } catch (Throwable ex) {
        log.error("Failed to consume element {}", m, ex);
        if (errorHandler.apply(ex)) {
          log.info("Restarting consumption by request.");
          stopAsync(subscriber).awaitTerminated();
          onRestart.run();
          subscriber.set(newSubscriber(subscription, receiver.get()));
          subscriber.get().startAsync().awaitRunning();
        } else {
          log.info("Terminating consumption after error.");
          stopAsync(subscriber);
        }
      }
    });

    subscriber.set(newSubscriber(subscription, receiver.get()));
    subscriber.get().startAsync();

    if (onInit != null) {
      executor().submit(() -> {
        subscriber.get().awaitRunning();
        onInit.run();
      });
    }

    return new ObserveHandle() {

      @Override
      public void cancel() {
        stopProcessing.set(true);
        Subscriber sub = stopAsync(subscriber);
        if (sub != null) {
          sub.awaitTerminated();
        }
        onCancel.run();
      }

      @Override
      public List<Offset> getCommittedOffsets() {
        return Arrays.asList(offset);
      }

      @Override
      public void resetOffsets(List<Offset> offsets) {
        // nop
      }

      @Override
      public List<Offset> getCurrentOffsets() {
        return getCommittedOffsets();
      }

      @Override
      public void waitUntilReady() throws InterruptedException {
        subscriber.get().awaitRunning();
      }
    };
  }

  Subscriber stopAsync(AtomicReference<Subscriber> subscriber) {
    return Optional.ofNullable(subscriber.getAndSet(null))
        .map(s -> {
          log.info("Closing subscriber {}", s);
          s.stopAsync();
          return s;
        })
        .orElse(null);
  }

  ExecutorService executor() {
    if (executor == null) {
      executor = context.getExecutorService();
    }
    return executor;
  }

  static Optional<StreamElement> toElement(EntityDescriptor entity, PubsubMessage m) {
    try {
      String uuid = m.getMessageId();
      ByteString data = m.getData();
      PubSub.KeyValue parsed = PubSub.KeyValue.parseFrom(data);
      long stamp = parsed.getStamp();
      Optional<AttributeDescriptor<Object>> attribute = entity.findAttribute(parsed.getAttribute());
      if (attribute.isPresent()) {
        if (parsed.getDelete()) {
          return Optional.of(StreamElement.delete(
              entity, attribute.get(), uuid,
              parsed.getKey(), parsed.getAttribute(), stamp));
        } else if (parsed.getDeleteWildcard()) {
          return Optional.of(StreamElement.deleteWildcard(
              entity, attribute.get(), uuid, parsed.getKey(), stamp));
        }
        return Optional.of(StreamElement.update(
            entity, attribute.get(), uuid, parsed.getKey(), parsed.getAttribute(),
            stamp, parsed.getValue().toByteArray()));
      }
      log.warn("Failed to find attribute {} in entity {}", parsed.getAttribute(), entity);
    } catch (InvalidProtocolBufferException ex) {
      log.warn("Failed to parse message {}", m, ex);
    }
    return Optional.empty();
  }

}
