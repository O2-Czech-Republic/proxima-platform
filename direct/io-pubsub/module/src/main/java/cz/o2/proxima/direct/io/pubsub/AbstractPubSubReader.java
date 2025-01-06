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

import static cz.o2.proxima.direct.core.commitlog.ObserverUtils.asOnNextContext;
import static cz.o2.proxima.direct.core.commitlog.ObserverUtils.asRepartitionContext;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.protobuf.FieldMask;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.UpdateSubscriptionRequest;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.time.WatermarkEstimator;
import cz.o2.proxima.core.time.WatermarkEstimatorFactory;
import cz.o2.proxima.core.time.WatermarkSupplier;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OffsetCommitter;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.commitlog.Offset;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.internal.com.google.common.collect.Lists;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.threeten.bp.Duration;

@Slf4j
public abstract class AbstractPubSubReader extends AbstractStorage {
  protected final PubSubAccessor accessor;
  protected final Context context;
  protected final String project;
  protected final String topic;
  protected final int maxAckDeadline;
  protected final int subscriptionAckDeadline;
  protected final boolean subscriptionAutoCreate;
  protected final WatermarkEstimatorFactory watermarkFactory;
  private @Nullable ExecutorService executor;

  protected AbstractPubSubReader(
      EntityDescriptor entityDesc, URI uri, PubSubAccessor accessor, Context context) {

    super(entityDesc, uri);
    this.accessor = accessor;
    this.context = context;
    this.project = accessor.getProject();
    this.topic = accessor.getTopic();
    this.maxAckDeadline = accessor.getMaxAckDeadline();
    this.subscriptionAckDeadline = accessor.getSubscriptionAckDeadline();
    this.subscriptionAutoCreate = accessor.isSubscriptionAutoCreate();
    this.watermarkFactory = accessor.getWatermarkConfiguration().getWatermarkEstimatorFactory();
  }

  public List<Partition> getPartitions() {
    // pubsub has only single (splittable) partition from the client perspective
    return List.of(new PubSubPartition(asConsumerName(null)));
  }

  public ObserveHandle observe(
      @Nullable String name, Position position, CommitLogObserver observer) {

    return observe(name, position, Long.MIN_VALUE, observer);
  }

  private ObserveHandle observe(
      @Nullable String name, Position position, long minWatermark, CommitLogObserver observer) {

    validatePosition(position);
    String consumerName = asConsumerName(name);
    AtomicLong committedWatermark = new AtomicLong(minWatermark);
    return consume(
        consumerName,
        (e, w, c) -> {
          OffsetCommitter committer =
              (succ, exc) -> {
                if (succ) {
                  log.debug("Confirming message {} to PubSub", e);
                  committedWatermark.set(w.getWatermark());
                  c.commit(true, null);
                } else {
                  if (exc != null) {
                    log.warn("Error during processing of {}", e, exc);
                  } else {
                    log.info("Nacking message {} by request", e);
                  }
                  c.commit(false, exc);
                }
              };
          try {
            long watermark = w.getWatermark();
            Offset offset = new PubSubOffset(consumerName, watermark);
            boolean ret = observer.onNext(e, asOnNextContext(committer, offset));
            if (!ret) {
              observer.onCompleted();
            }
            return ret;
          } catch (Exception ex) {
            log.error("Error calling onNext", ex);
            committer.fail(ex);
            throw new IllegalStateException(ex);
          }
        },
        observer::onError,
        null,
        () -> {},
        observer::onCancelled,
        committedWatermark);
  }

  public ObserveHandle observePartitions(
      @Nullable String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    validateNotStopAtCurrent(stopAtCurrent);
    name = findConsumerFromPartitions(name, partitions);
    return observe(name, position, observer);
  }

  /**
   * Observe PubSub in a bulk fashion. Note that due to current PubSub implementation the bulk
   * commit must happen before the ack timeout. If the message is not acknowledged before this
   * timeout the message will be redelivered, which will result in duplicate messages.
   *
   * @param name name of the observer subscription
   * @param position must be set to NEWEST
   * @param stopAtCurrent throw {@link UnsupportedOperationException} when {@code true}
   * @param observer the observer of data
   * @return handle to interact with the observation thread
   */
  public ObserveHandle observeBulk(
      @Nullable String name, Position position, boolean stopAtCurrent, CommitLogObserver observer) {

    return observeBulk(name, position, stopAtCurrent, Long.MIN_VALUE, observer);
  }

  private ObserveHandle observeBulk(
      @Nullable String name,
      Position position,
      boolean stopAtCurrent,
      long minWatermark,
      CommitLogObserver observer) {

    validateNotStopAtCurrent(stopAtCurrent);

    validatePosition(position);
    AtomicReference<List<CommitCallback>> unconfirmed = new AtomicReference<>(new ArrayList<>());
    Object lock = new Object();
    Object listLock = new Object();
    AtomicLong globalOffset = new AtomicLong();
    String consumerName = asConsumerName(name);
    AtomicLong committedWatermark = new AtomicLong(minWatermark);
    PubSubPartition partition = new PubSubPartition(consumerName);
    return consume(
        consumerName,
        (e, w, c) -> {
          final long confirmUntil;
          synchronized (listLock) {
            List<CommitCallback> list = unconfirmed.get();
            list.add(c);
            confirmUntil = list.size() + globalOffset.get();
          }
          OffsetCommitter committer =
              createBulkCommitter(
                  listLock, confirmUntil, globalOffset, unconfirmed, w, committedWatermark);

          // our observers are not supposed to be thread safe, so we must
          // ensure explicit synchronization here
          synchronized (lock) {
            try {
              Offset offset = new PubSubOffset(consumerName, w.getWatermark());
              if (!observer.onNext(e, asOnNextContext(committer, offset))) {
                observer.onCompleted();
                return false;
              }
              return true;
            } catch (Exception ex) {
              log.error("Error calling on next", ex);
              committer.fail(ex);
              throw new IllegalStateException(ex);
            }
          }
        },
        observer::onError,
        () -> observer.onRepartition(asRepartitionContext(List.of(partition))),
        () -> observer.onRepartition(asRepartitionContext(List.of(partition))),
        observer::onCancelled,
        committedWatermark);
  }

  private OffsetCommitter createBulkCommitter(
      Object listLock,
      long confirmUntil,
      AtomicLong globalOffset,
      AtomicReference<List<CommitCallback>> unconfirmed,
      WatermarkSupplier watermarkSupplier,
      AtomicLong committedWatermark) {

    return (succ, exc) -> {
      // the implementation can use some other
      // thread for this, so we need to synchronize this
      synchronized (listLock) {
        int confirmCount = (int) (confirmUntil - globalOffset.get());
        if (confirmCount > 0) {
          final Consumer<CommitCallback> apply;
          if (succ) {
            log.debug("Bulk confirming {} messages", confirmCount);
            apply = c -> c.commit(true, null);
            committedWatermark.set(watermarkSupplier.getWatermark());
          } else {
            if (exc != null) {
              log.warn("Error during processing of last bulk", exc);
            } else {
              log.info("Nacking last bulk by request");
            }
            apply = c -> c.commit(false, exc);
          }
          List<CommitCallback> list = unconfirmed.get();
          for (int i = 0; i < confirmCount; i++) {
            apply.accept(list.get(i));
          }
          globalOffset.addAndGet(confirmCount);
          unconfirmed.set(Lists.newArrayList(list.subList(confirmCount, list.size())));
        }
      }
    };
  }

  public ObserveHandle observeBulkPartitions(
      @Nullable String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      CommitLogObserver observer) {

    name = findConsumerFromPartitions(name, partitions);
    return observeBulkWithMinWatermark(name, position, stopAtCurrent, Long.MIN_VALUE, observer);
  }

  private ObserveHandle observeBulkWithMinWatermark(
      @Nullable String name,
      Position position,
      boolean stopAtCurrent,
      long minWatermark,
      CommitLogObserver observer) {

    validateNotStopAtCurrent(stopAtCurrent);

    return observeBulk(name, position, false, minWatermark, observer);
  }

  public ObserveHandle observeBulkOffsets(
      Collection<Offset> offsets, boolean stopAtCurrent, CommitLogObserver observer) {

    List<String> names =
        offsets.stream()
            .map(o -> ((PubSubOffset) o).getConsumerName())
            .distinct()
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        names.size() == 1, "Offsets should be reading same consumer, got %s", names);
    String name = Iterables.getOnlyElement(names);
    long watermark =
        offsets.stream()
            .mapToLong(o -> ((PubSubOffset) o).getWatermark())
            .min()
            .orElse(Long.MIN_VALUE);
    return observeBulkWithMinWatermark(
        asConsumerName(name), Position.NEWEST, stopAtCurrent, watermark, observer);
  }

  @VisibleForTesting
  Subscriber newSubscriber(ProjectSubscriptionName subscription, MessageReceiver receiver) {

    if (subscriptionAutoCreate) {
      try (SubscriptionAdminClient client = SubscriptionAdminClient.create()) {
        createSubscription(client, subscription);
      } catch (Exception ex) {
        throw new IllegalStateException(ex);
      }
    }
    return Subscriber.newBuilder(subscription, receiver)
        .setMaxAckExtensionPeriod(Duration.ofMillis(maxAckDeadline))
        .setFlowControlSettings(
            FlowControlSettings.newBuilder()
                .setLimitExceededBehavior(LimitExceededBehavior.Ignore)
                .build())
        .build();
  }

  WatermarkEstimator createWatermarkEstimator(long minWatermark) {
    WatermarkEstimator res = watermarkFactory.create();
    res.setMinWatermark(minWatermark);
    return res;
  }

  private void createSubscription(
      SubscriptionAdminClient client, ProjectSubscriptionName subscription) {

    try {
      ProjectTopicName topicName = ProjectTopicName.of(project, topic);
      client.createSubscription(
          subscription, topicName, PushConfig.newBuilder().build(), this.subscriptionAckDeadline);
      log.info(
          "Automatically creating subscription {} for topic {} with ackDeadline {}"
              + " as requested",
          subscription,
          topicName,
          subscriptionAckDeadline);
    } catch (AlreadyExistsException ex) {
      Subscription subs = client.getSubscription(subscription);
      if (!subs.getTopic().equals(ProjectTopicName.of(project, topic).toString())) {
        throw new IllegalStateException(
            "Existed subscription "
                + subscription.getSubscription()
                + " use topic "
                + subs.getTopic()
                + " which is different than configured "
                + ProjectTopicName.of(project, topic)
                + ".");
      }
      if (subs.getAckDeadlineSeconds() != this.subscriptionAckDeadline) {
        client.updateSubscription(
            UpdateSubscriptionRequest.newBuilder()
                .setUpdateMask(FieldMask.newBuilder().addPaths("ack_deadline_seconds").build())
                .setSubscription(
                    Subscription.newBuilder()
                        .setAckDeadlineSeconds(this.subscriptionAckDeadline)
                        .setName(subscription.toString())
                        .build())
                .build());
        log.info(
            "Subscription ack deadline {} for subscription {} was different than "
                + "configured: {}. Subscription updated.",
            subs.getAckDeadlineSeconds(),
            subscription,
            this.subscriptionAckDeadline);
      } else {
        log.debug("Subscription {} already exists. Skipping creation.", subscription);
      }
    }
  }

  private void validatePosition(Position position) {
    if (position == Position.OLDEST) {
      failUnsupported();
    }
  }

  private void validateNotStopAtCurrent(boolean stopAtCurrent) {
    if (stopAtCurrent) {
      failUnsupported();
    }
  }

  private void failUnsupported() {
    throw new UnsupportedOperationException("PubSub can observe only current data.");
  }

  private String asConsumerName(String name) {
    return name != null ? name : "unnamed-consumer-" + UUID.randomUUID().toString();
  }

  private ObserveHandle consume(
      String consumerName,
      PubSubConsumer consumer,
      UnaryFunction<Throwable, Boolean> errorHandler,
      @Nullable Runnable onInit,
      Runnable onRestart,
      Runnable onCancel,
      AtomicLong committedWatermark) {

    ProjectSubscriptionName subscription = ProjectSubscriptionName.of(project, consumerName);

    AtomicReference<Subscriber> subscriber = new AtomicReference<>();
    AtomicBoolean stopProcessing = new AtomicBoolean();
    AtomicReference<MessageReceiver> receiver = new AtomicReference<>();
    WatermarkEstimator watermarkEstimator = createWatermarkEstimator(committedWatermark.get());
    receiver.set(
        createMessageReceiver(
            subscription,
            subscriber,
            stopProcessing,
            consumer,
            watermarkEstimator,
            errorHandler,
            onRestart,
            receiver));

    subscriber.set(newSubscriber(subscription, receiver.get()));
    subscriber.get().startAsync();

    if (onInit != null) {
      executor()
          .submit(
              () -> {
                subscriber.get().awaitRunning();
                if (onInit != null) {
                  onInit.run();
                }
              });
    }

    return new ObserveHandle() {

      @Override
      public void close() {
        log.debug("Cancelling observer {}", consumerName);
        stopProcessing.set(true);
        Subscriber sub = stopAsync(subscriber);
        if (sub != null) {
          sub.awaitTerminated();
        }
        onCancel.run();
      }

      @Override
      public List<Offset> getCommittedOffsets() {
        return Collections.singletonList(new PubSubOffset(consumerName, committedWatermark.get()));
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
      public void waitUntilReady() {
        subscriber.get().awaitRunning();
      }
    };
  }

  private MessageReceiver createMessageReceiver(
      ProjectSubscriptionName subscription,
      AtomicReference<Subscriber> subscriber,
      AtomicBoolean stopProcessing,
      PubSubConsumer consumer,
      WatermarkEstimator watermarkEstimator,
      UnaryFunction<Throwable, Boolean> errorHandler,
      Runnable onRestart,
      AtomicReference<MessageReceiver> receiver) {

    return (m, c) -> {
      try {
        log.trace("Received message {}", m);
        if (stopProcessing.get()) {
          log.debug("Returning rejected message {}", m);
          c.nack();
          return;
        }
        List<StreamElement> elements = parseElements(m);
        processParsedElements(elements, subscriber, consumer, watermarkEstimator, m, c);
      } catch (Throwable ex) {
        log.error("Failed to consume element {}", m, ex);
        if (Boolean.TRUE.equals(errorHandler.apply(ex))) {
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
    };
  }

  private void processParsedElements(
      List<StreamElement> elements,
      AtomicReference<Subscriber> subscriber,
      PubSubConsumer consumer,
      WatermarkEstimator watermarkEstimator,
      PubsubMessage m,
      AckReplyConsumer c) {

    if (!elements.isEmpty()) {
      CommitCallback partial =
          CommitCallback.afterNumCommits(
              elements.size(),
              (succ, exc) -> {
                if (succ) {
                  c.ack();
                } else {
                  c.nack();
                }
              });
      elements.forEach(
          elem -> {
            long current = watermarkEstimator.getWatermark();
            watermarkEstimator.update(elem);
            if (watermarkEstimator.getWatermark() < current) {
              log.warn(
                  "Element {} is moving watermark backwards of {} ms. "
                      + "If this happens too often, then it is likely you need to extend "
                      + "ack deadline.",
                  elem,
                  current - watermarkEstimator.getWatermark());
            }
            if (!consumer.consume(elem, watermarkEstimator, partial)) {
              log.info("Terminating consumption by request.");
              stopAsync(subscriber);
            }
          });
    } else {
      log.warn("Skipping unparseable element {}", m);
      c.ack();
    }
  }

  protected abstract List<StreamElement> parseElements(PubsubMessage m);

  Subscriber stopAsync(AtomicReference<Subscriber> subscriber) {
    return Optional.ofNullable(subscriber.getAndSet(null))
        .map(
            s -> {
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

  public boolean hasExternalizableOffsets() {
    // all offsets represent the same read position
    return false;
  }

  private String findConsumerFromPartitions(String name, Collection<Partition> partitions) {

    if (name != null) {
      return name;
    }
    Set<String> names =
        partitions.stream()
            .map(p -> ((PubSubPartition) p).getConsumerName())
            .collect(Collectors.toSet());
    Preconditions.checkArgument(
        names.size() == 1,
        "Please provide partitions originating from single #split partition. Got %s",
        partitions);
    return Iterables.getOnlyElement(names);
  }

  @FunctionalInterface
  private interface PubSubConsumer extends Serializable {
    boolean consume(StreamElement elem, WatermarkSupplier watermark, CommitCallback committer);
  }

  @Getter
  @Value
  static class PubSubPartition implements Partition {

    String consumerName;

    @Override
    public int getId() {
      return 0;
    }

    @Override
    public boolean isSplittable() {
      return true;
    }

    @Override
    public Collection<Partition> split(int desiredCount) {
      log.info("Splitting partition {} into {} parts", this, desiredCount);
      return IntStream.range(0, desiredCount).mapToObj(i -> this).collect(Collectors.toList());
    }

    @Override
    public String toString() {
      return "PubSubPartition(" + consumerName + ")";
    }
  }

  @VisibleForTesting
  @Value
  @Getter
  static class PubSubOffset implements Offset {

    String consumerName;
    long watermark;

    @Override
    public Partition getPartition() {
      return new PubSubPartition(consumerName);
    }

    @Override
    public String toString() {
      return "PubSubOffset(consumerName=" + consumerName + ", watermark=" + watermark + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof PubSubOffset) {
        PubSubOffset off = (PubSubOffset) obj;
        return off.consumerName.equals(consumerName) && off.watermark == watermark;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(consumerName, watermark);
    }
  }
}
