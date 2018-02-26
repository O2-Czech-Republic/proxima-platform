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
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
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
import cz.seznam.euphoria.shadow.com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.threeten.bp.Duration;

/**
 * A {@link CommitLogReader} for Google PubSub.
 */
@Slf4j
class PubSubReader extends AbstractStorage implements CommitLogReader {

  private static class PubSubOffset implements Offset {
    @Override
    public Partition getPartition() {
      return () -> 0;
    }
  }

  private final String project;
  private final String topic;
  private final PubSubOffset offset = new PubSubOffset();
  private final int maxAckDeadline;
  private final int subscriptionAckDeadline;
  private final boolean subscriptionAutoCreate;

  PubSubReader(PubSubAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getURI());
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
  public ObserveHandle observe(String name, Position position, LogObserver observer) {
    validatePosition(position);
    return consume(name, (e, c) -> {
      boolean ret = observer.onNext(e, (succ, exc) -> {
        if (succ) {
          log.debug("Confirming message {} to PubSub", e);
          c.ack();
        } else {
          log.warn("Error during processing of {}", e, exc);
          c.nack();
        }
      });
      if (!ret) {
        observer.onCompleted();
      }
      return ret;
    }, observer::onError, observer::onCancelled);
  }

  @Override
  public ObserveHandle observePartitions(
      Collection<Partition> partitions, Position position,
      boolean stopAtCurrent, LogObserver observer) {

    if (stopAtCurrent) {
      throw new UnsupportedOperationException(
          "PubSub can observe only current data.");
    }
    return observe(
        "unnamed-consumer-" + UUID.randomUUID().toString(),
        position, observer);
  }

  /**
   * Observe PubSub in a bulk fashion.
   * Note that due to current PubSub implementation the bulk commit must
   * happen before the ack timeout. If the message is not acknowledged before this
   * timeout the message will be redelivered, which will result in duplicate
   * messages.
   * @param name name of the observer subscription
   * @param position must be set to NEWEST
   * @param observer the observer of data
   * @return handle to interact with the observation thread
   */
  @Override
  public ObserveHandle observeBulk(
      String name, Position position, BulkLogObserver observer) {

    validatePosition(position);
    List<AckReplyConsumer> unconfirmed = Collections.synchronizedList(new ArrayList<>());
    return consume(name, (e, c) -> {
      unconfirmed.add(c);
      boolean ret = observer.onNext(e, () -> 0, (succ, exc) -> {
        synchronized (unconfirmed) {
          if (succ) {
            log.debug("Bulk confirming {} messages", unconfirmed.size());
            unconfirmed.forEach(AckReplyConsumer::ack);
          } else {
            log.warn("Error during processing of last bulk", exc);
            unconfirmed.forEach(AckReplyConsumer::nack);
          }
          unconfirmed.clear();
        }
      });
      if (!ret) {
        observer.onCompleted();
      }
      return ret;
    }, observer::onError, observer::onCancelled);
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      Collection<Partition> partitions, Position position, BulkLogObserver observer) {

    return observeBulk(
        "unnamed-bulk-consumer-" + UUID.randomUUID().toString(),
        position, observer);
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
  Subscriber newSubscriber(SubscriptionName subscription, MessageReceiver receiver) {
    if (subscriptionAutoCreate) {
      try (SubscriptionAdminClient client = SubscriptionAdminClient.create()) {
        try {
          client.createSubscription(
              subscription, TopicName.of(project, topic),
              PushConfig.newBuilder().build(), this.subscriptionAckDeadline);
          log.info(
              "Automatically creating subscription {} for topic {} as requested",
              subscription.getSubscription(), topic);
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
      String name,
      BiFunction<StreamElement, AckReplyConsumer, Boolean> consumer,
      Function<Throwable, Boolean> errorHandler,
      Runnable cancel) {

    SubscriptionName subscription = SubscriptionName.of(project, name);
    AtomicReference<Subscriber> subscriber = new AtomicReference<>();
    AtomicReference<MessageReceiver> receiver = new AtomicReference<>();
    receiver.set((m, c) -> {
      try {
        Optional<StreamElement> elem = toElement(getEntityDescriptor(), m);
        if (elem.isPresent()) {
          if (!consumer.apply(elem.get(), c)) {
            log.info("Terminating consumption by request.");
            subscriber.get().stopAsync();
          }
        } else {
          log.warn("Skipping unparseable element {}", m);
          c.ack();
        }
      } catch (Throwable ex) {
        log.error("Failed to consume element {}", m, ex);
        if (errorHandler.apply(ex)) {
          log.info("Restarting consumption by request.");
          subscriber.get().stopAsync();
          subscriber.set(newSubscriber(subscription, receiver.get()));
          subscriber.get().startAsync().awaitRunning();
        } else {
          log.info("Terminating consumption after error.");
          subscriber.get().stopAsync();
        }
      }
    });
    subscriber.set(newSubscriber(subscription, receiver.get()));
    subscriber.get().startAsync().awaitRunning();

    return new ObserveHandle() {

      @Override
      public void cancel() {
        subscriber.get().stopAsync();
        cancel.run();
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
        // nop
      }
    };
  }

  static Optional<StreamElement> toElement(EntityDescriptor entity, PubsubMessage m) {
    try {
      long stamp = m.getPublishTime().getSeconds() * 1000L
          + m.getPublishTime().getNanos() / 1_000_000L;
      String uuid = m.getMessageId();
      ByteString data = m.getData();
      PubSub.KeyValue parsed = PubSub.KeyValue.parseFrom(data);
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
