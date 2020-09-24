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
package cz.o2.proxima.direct.kafka;

import static java.util.stream.Collectors.toMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.kafka.Consumers.BulkConsumer;
import cz.o2.proxima.direct.kafka.Consumers.OnlineConsumer;
import cz.o2.proxima.direct.time.MinimalPartitionWatermarkEstimator;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.PartitionedWatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimator;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import cz.o2.proxima.time.Watermarks;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/** A {@link CommitLogReader} implementation for Kafka. */
@Slf4j
public class KafkaLogReader extends AbstractStorage implements CommitLogReader {

  @Getter final KafkaAccessor accessor;
  @Getter private final Context context;
  private final long consumerPollInterval;
  private final long maxBytesPerSec;
  private final String topic;
  private final Map<String, Object> cfg;

  KafkaLogReader(KafkaAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getUri());
    this.accessor = accessor;
    this.context = context;
    this.consumerPollInterval = accessor.getConsumerPollInterval();
    this.maxBytesPerSec = accessor.getMaxBytesPerSec();
    this.topic = accessor.getTopic();
    this.cfg = accessor.getCfg();

    log.debug("Created {} for accessor {}", getClass().getSimpleName(), accessor);
  }

  /**
   * Subscribe observer by name to the commit log. Each observer maintains its own position in the
   * commit log, so that the observers with different names do not interfere If multiple observers
   * share the same name, then the ingests are load-balanced between them (in an undefined manner).
   * This is a non blocking call.
   *
   * @param name identifier of the consumer
   */
  @Override
  public ObserveHandle observe(String name, Position position, LogObserver observer) {

    return observeKafka(name, null, position, false, observer);
  }

  @Override
  public ObserveHandle observePartitions(
      String name,
      @Nullable Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    return observeKafka(null, partitions, position, stopAtCurrent, observer);
  }

  @Override
  public ObserveHandle observeBulk(
      String name, Position position, boolean stopAtCurrent, LogObserver observer) {

    return observeKafkaBulk(name, null, position, stopAtCurrent, observer);
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    // name is ignored, because when observing partition the offsets
    // are not committed to kafka
    return observeKafkaBulk(
        null, createDefaultOffsets(partitions), position, stopAtCurrent, observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(Collection<Offset> offsets, LogObserver observer) {

    return observeKafkaBulk(null, offsets, Position.CURRENT, false, observer);
  }

  @Override
  public List<Partition> getPartitions() {
    final List<PartitionInfo> partitions;
    try (KafkaConsumer<Object, Object> consumer = createConsumer()) {
      partitions = consumer.partitionsFor(topic);
    }
    return partitions.stream().map(p -> Partition.of(p.partition())).collect(Collectors.toList());
  }

  @VisibleForTesting
  ObserveHandle observeKafka(
      @Nullable String name,
      @Nullable Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    try {
      return processConsumer(
          name,
          createDefaultOffsets(partitions),
          position,
          stopAtCurrent,
          name != null,
          observer,
          context.getExecutorService());
    } catch (InterruptedException ex) {
      log.warn("Interrupted waiting for kafka observer to start", ex);
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }
  }

  private ObserveHandle observeKafkaBulk(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    Preconditions.checkArgument(
        name != null || offsets != null, "Either name or offsets have to be non null");

    Preconditions.checkArgument(position != null, "Position cannot be null");

    try {
      return processConsumerBulk(
          name,
          offsets,
          position,
          stopAtCurrent,
          name != null,
          observer,
          context.getExecutorService());
    } catch (InterruptedException ex) {
      log.warn("Interrupted waiting for kafka observer to start", ex);
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }
  }

  /**
   * Process given consumer in online fashion.
   *
   * @param name name of the consumer
   * @param offsets assigned offsets
   * @param position where to read from
   * @param stopAtCurrent termination flag
   * @param commitToKafka should we commit to kafka
   * @param observer the observer
   * @param executor executor to use for async processing
   * @return observe handle
   */
  @VisibleForTesting
  ObserveHandle processConsumer(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      Position position,
      boolean stopAtCurrent,
      boolean commitToKafka,
      LogObserver observer,
      ExecutorService executor)
      throws InterruptedException {

    // offsets that should be committed to kafka
    Map<TopicPartition, OffsetAndMetadata> kafkaCommitMap;
    kafkaCommitMap = Collections.synchronizedMap(new HashMap<>());

    final OffsetCommitter<TopicPartition> offsetCommitter = createOffsetCommitter();

    final BiConsumer<TopicPartition, ConsumerRecord<Object, Object>> preWrite =
        (tp, r) -> {
          final long offset = r.offset();
          offsetCommitter.register(
              tp,
              offset,
              1,
              () -> {
                OffsetAndMetadata mtd = new OffsetAndMetadata(offset + 1);
                if (commitToKafka) {
                  kafkaCommitMap.put(tp, mtd);
                }
              });
        };

    OnlineConsumer<Object, Object> onlineConsumer =
        new OnlineConsumer<>(
            observer,
            offsetCommitter,
            () -> {
              synchronized (kafkaCommitMap) {
                Map<TopicPartition, OffsetAndMetadata> clone = new HashMap<>(kafkaCommitMap);
                kafkaCommitMap.clear();
                return clone;
              }
            });

    AtomicReference<ObserveHandle> handle = new AtomicReference<>();
    submitConsumerWithObserver(
        name, offsets, position, stopAtCurrent, preWrite, onlineConsumer, executor, handle);
    return dynamicHandle(handle);
  }

  /**
   * Process given consumer in bulk fashion.
   *
   * @param name name of the consumer
   * @param offsets assigned offsets
   * @param position where to read from
   * @param stopAtCurrent termination flag
   * @param commitToKafka should we commit to kafka
   * @param observer the observer
   * @param executor executor to use for async processing
   * @return observe handle
   */
  @VisibleForTesting
  ObserveHandle processConsumerBulk(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      Position position,
      boolean stopAtCurrent,
      boolean commitToKafka,
      LogObserver observer,
      ExecutorService executor)
      throws InterruptedException {

    // offsets that should be committed to kafka
    Map<TopicPartition, OffsetAndMetadata> kafkaCommitMap;
    kafkaCommitMap = Collections.synchronizedMap(new HashMap<>());

    @SuppressWarnings("unchecked")
    final BulkConsumer<Object, Object> bulkConsumer =
        new BulkConsumer<>(
            topic,
            observer,
            (tp, o) -> {
              if (commitToKafka) {
                OffsetAndMetadata off = new OffsetAndMetadata(o);
                kafkaCommitMap.put(tp, off);
              }
            },
            () -> {
              synchronized (kafkaCommitMap) {
                Map<TopicPartition, OffsetAndMetadata> clone = new HashMap<>(kafkaCommitMap);
                kafkaCommitMap.clear();
                return clone;
              }
            },
            kafkaCommitMap::clear);

    AtomicReference<ObserveHandle> handle = new AtomicReference<>();
    submitConsumerWithObserver(
        name, offsets, position, stopAtCurrent, (tp, r) -> {}, bulkConsumer, executor, handle);
    return dynamicHandle(handle);
  }

  private void submitConsumerWithObserver(
      final @Nullable String name,
      final @Nullable Collection<Offset> offsets,
      final Position position,
      boolean stopAtCurrent,
      final BiConsumer<TopicPartition, ConsumerRecord<Object, Object>> preWrite,
      final ElementConsumer<Object, Object> consumer,
      final ExecutorService executor,
      final AtomicReference<ObserveHandle> handle)
      throws InterruptedException {

    final CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean completed = new AtomicBoolean();
    AtomicBoolean shutdown = new AtomicBoolean();
    List<TopicOffset> seekOffsets = Collections.synchronizedList(new ArrayList<>());

    executor.submit(
        () -> {
          handle.set(createObserveHandle(shutdown, seekOffsets, consumer, latch));
          final AtomicReference<KafkaConsumer<Object, Object>> consumerRef;
          final AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator =
              new AtomicReference<>(null);
          final Map<Integer, Integer> emptyPollCount = new ConcurrentHashMap<>();
          final Duration pollDuration = Duration.ofMillis(consumerPollInterval);
          consumerRef = new AtomicReference<>();
          consumer.onStart();
          ConsumerRebalanceListener listener =
              listener(name, consumerRef, consumer, emptyPollCount, watermarkEstimator);
          final ElementSerializer<Object, Object> serializer = accessor.getSerializer();

          try (KafkaConsumer<Object, Object> kafka =
              createConsumer(name, offsets, name != null ? listener : null, position)) {

            consumerRef.set(kafka);

            // we need to poll first to initialize kafka assignments and rebalance listener
            ConsumerRecords<Object, Object> poll = kafka.poll(pollDuration);

            Map<TopicPartition, Long> endOffsets =
                stopAtCurrent ? findNonEmptyEndOffsets(kafka) : null;

            if (log.isDebugEnabled()) {
              log.debug("End offsets of current assignment {}: {}", kafka.assignment(), endOffsets);
            }

            if (offsets != null) {
              // when manual offsets are assigned, we need to ensure calling
              // onAssign by hand
              listener.onPartitionsAssigned(kafka.assignment());
            }

            latch.countDown();

            AtomicReference<Throwable> error = new AtomicReference<>();
            do {
              if (poll.isEmpty()) {
                Optional.ofNullable(watermarkEstimator.get()).ifPresent(consumer::onIdle);
              }
              logConsumerWatermark(name, offsets, watermarkEstimator, poll.count());
              poll =
                  seekToNewOffsetsIfNeeded(seekOffsets, consumer, watermarkEstimator, kafka, poll);

              final long bytesPerPoll =
                  maxBytesPerSec < Long.MAX_VALUE
                      ? Math.max(1L, maxBytesPerSec / (1000L * consumerPollInterval))
                      : Long.MAX_VALUE;
              long bytesPolled = 0L;
              // increase all partition's empty poll counter by 1
              emptyPollCount.replaceAll((k, v) -> v + 1);
              for (ConsumerRecord<Object, Object> r : poll) {
                bytesPolled += r.serializedKeySize() + r.serializedValueSize();
                TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                emptyPollCount.put(tp.partition(), 0);
                preWrite.accept(tp, r);
                StreamElement ingest = serializer.read(r, getEntityDescriptor());
                if (ingest != null) {
                  watermarkEstimator.get().update(tp.partition(), ingest);
                }
                boolean cont =
                    consumer.consumeWithConfirm(
                        ingest, tp, r.offset(), watermarkEstimator.get(), error::set);
                if (!cont) {
                  log.info("Terminating consumption by request");
                  completed.set(true);
                  break;
                }
                if (stopAtCurrent) {
                  Long end = endOffsets.get(tp);
                  if (end != null && end - 1 <= r.offset()) {
                    log.debug("Reached end of partition {} at offset {}", tp, r.offset());
                    endOffsets.remove(tp);
                  }
                }
              }
              increaseWatermarkOnEmptyPolls(emptyPollCount, watermarkEstimator);
              flushCommits(kafka, consumer);
              rethrowErrorIfPresent(name, error);
              terminateIfConsumed(stopAtCurrent, kafka, endOffsets, completed);
              waitToReduceThroughput(bytesPolled, bytesPerPoll);
              poll = kafka.poll(pollDuration);
            } while (!shutdown.get()
                && !completed.get()
                && !Thread.currentThread().isInterrupted());
            if (log.isDebugEnabled()) {
              log.debug(
                  "Terminating poll loop for assignment {}: shutdown: {}, completed: {}, interrupted: {}",
                  kafka.assignment(),
                  shutdown.get(),
                  completed.get(),
                  Thread.currentThread().isInterrupted());
            }
            if (!Thread.currentThread().isInterrupted()) {
              consumer.onCompleted();
            } else {
              consumer.onCancelled();
            }
          } catch (InterruptedException ex) {
            log.info("Interrupted while polling kafka. Terminating consumption.", ex);
            Thread.currentThread().interrupt();
            consumer.onCancelled();
          } catch (Throwable err) {
            log.error("Error processing consumer {}", name, err);
            if (consumer.onError(err)) {
              try {
                submitConsumerWithObserver(
                    name, offsets, position, stopAtCurrent, preWrite, consumer, executor, handle);
              } catch (InterruptedException ex) {
                log.warn("Interrupted while restarting observer");
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
              }
            }
          }
        });
    latch.await();
  }

  private ConsumerRecords<Object, Object> seekToNewOffsetsIfNeeded(
      final List<TopicOffset> seekOffsets,
      final ElementConsumer<Object, Object> consumer,
      final AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator,
      final KafkaConsumer<Object, Object> kafka,
      final ConsumerRecords<Object, Object> poll) {

    synchronized (seekOffsets) {
      if (!seekOffsets.isEmpty()) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        List<Offset> toSeek = (List) seekOffsets;
        Utils.seekToOffsets(topic, toSeek, kafka);
        consumer.onAssign(
            kafka,
            kafka
                .assignment()
                .stream()
                .map(
                    tp ->
                        new TopicOffset(
                            tp.partition(),
                            kafka.position(tp),
                            watermarkEstimator.get().getWatermark()))
                .collect(Collectors.toList()));
        log.info("Seeked consumer to offsets {} as requested", seekOffsets);
        seekOffsets.clear();
        return ConsumerRecords.empty();
      }
    }
    return poll;
  }

  private void logConsumerWatermark(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator,
      int polledCount) {

    if (log.isDebugEnabled()) {
      log.debug(
          "Current watermark of consumer name {} with offsets {} " + "on {} poll'd records is {}",
          name,
          offsets,
          polledCount,
          Optional.ofNullable(watermarkEstimator.get())
              .map(PartitionedWatermarkEstimator::getWatermark)
              .orElse(Watermarks.MIN_WATERMARK));
    }
  }

  private void rethrowErrorIfPresent(
      @Nullable String consumerName, AtomicReference<Throwable> error) {
    Throwable errorThrown = error.getAndSet(null);
    if (errorThrown != null) {
      log.warn("Error during processing {}", consumerName, errorThrown);
      throw new RuntimeException(errorThrown);
    }
  }

  private void terminateIfConsumed(
      boolean stopAtCurrent,
      KafkaConsumer<?, ?> consumer,
      Map<TopicPartition, Long> endOffsets,
      AtomicBoolean completed) {

    if (stopAtCurrent && endOffsets.isEmpty()) {
      log.info(
          "Assignment {} reached end of current data. Terminating consumption.",
          consumer.assignment());
      completed.set(true);
    }
  }

  private void waitToReduceThroughput(long bytesPolled, final long bytesPerPoll)
      throws InterruptedException {

    long sleepDuration = bytesPolled * consumerPollInterval / bytesPerPoll;
    if (sleepDuration > 0) {
      TimeUnit.MILLISECONDS.sleep(sleepDuration);
    }
  }

  private void flushCommits(
      final KafkaConsumer<Object, Object> kafka, ElementConsumer<?, ?> consumer) {

    Map<TopicPartition, OffsetAndMetadata> commitMapClone;
    commitMapClone = consumer.prepareOffsetsForCommit();
    if (!commitMapClone.isEmpty()) {
      kafka.commitSync(commitMapClone);
    }
  }

  private void increaseWatermarkOnEmptyPolls(
      Map<Integer, Integer> emptyPollCount,
      AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator) {

    // we have to poll at least number of assigned partitions-times and still have empty poll
    // on that partition to be sure that it is actually empty
    int numEmptyPolls = emptyPollCount.size();
    emptyPollCount
        .entrySet()
        .stream()
        .filter(e -> e.getValue() >= numEmptyPolls)
        .forEach(e -> watermarkEstimator.get().idle(e.getKey()));
  }

  private ObserveHandle createObserveHandle(
      AtomicBoolean shutdown,
      List<TopicOffset> seekOffsets,
      ElementConsumer<?, ?> consumer,
      CountDownLatch latch) {

    return new ObserveHandle() {

      @Override
      public void close() {
        shutdown.set(true);
      }

      @SuppressWarnings("unchecked")
      @Override
      public List<Offset> getCommittedOffsets() {
        return (List) consumer.getCommittedOffsets();
      }

      @SuppressWarnings("unchecked")
      @Override
      public void resetOffsets(List<Offset> offsets) {
        seekOffsets.addAll((Collection) offsets);
      }

      @SuppressWarnings("unchecked")
      @Override
      public List<Offset> getCurrentOffsets() {
        return (List) consumer.getCurrentOffsets();
      }

      @Override
      public void waitUntilReady() throws InterruptedException {
        latch.await();
      }
    };
  }

  private Map<TopicPartition, Long> findNonEmptyEndOffsets(
      final KafkaConsumer<Object, Object> kafka) {

    Set<TopicPartition> assignment = kafka.assignment();
    Map<TopicPartition, Long> beginning;

    beginning = kafka.beginningOffsets(assignment);
    return kafka
        .endOffsets(assignment)
        .entrySet()
        .stream()
        .filter(entry -> beginning.get(entry.getKey()) < entry.getValue())
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private KafkaConsumer<Object, Object> createConsumer() {
    return createConsumer(UUID.randomUUID().toString(), null, null, Position.NEWEST);
  }

  /** Create kafka consumer for the data. */
  @VisibleForTesting
  KafkaConsumer<Object, Object> createConsumer(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      @Nullable ConsumerRebalanceListener listener,
      Position position) {

    Preconditions.checkArgument(
        name != null || listener == null,
        "Please use either named group (with listener) or offsets without listener");
    KafkaConsumerFactory<Object, Object> factory = accessor.createConsumerFactory();
    final KafkaConsumer<Object, Object> consumer;

    if ("".equals(name)) {
      throw new IllegalArgumentException("Consumer group cannot be empty string");
    }
    if (name != null) {
      consumer = factory.create(name, listener);
    } else if (offsets != null) {
      List<Partition> partitions =
          offsets.stream().map(Offset::getPartition).collect(Collectors.toList());
      consumer = factory.create(partitions);
    } else {
      throw new IllegalArgumentException("Need either name or offsets to observe");
    }
    validateTopic(consumer, topic);
    if (position == Position.OLDEST) {
      // seek all partitions to oldest data
      if (offsets == null) {
        if (consumer.assignment().isEmpty()) {
          // If we don't find assignment within timeout, poll results in IllegalStateException.
          // https://cwiki.apache.org/confluence/display/KAFKA/KIP-266%3A+Fix+consumer+indefinite+blocking+behavior
          consumer.poll(Duration.ofMillis(accessor.getAssignmentTimeoutMillis()));
        }
        Set<TopicPartition> assignment = consumer.assignment();
        log.info("Seeking consumer name {} to beginning of partitions {}", name, assignment);
        consumer.seekToBeginning(assignment);
      } else {
        List<TopicPartition> tps =
            offsets
                .stream()
                .map(p -> new TopicPartition(topic, p.getPartition().getId()))
                .collect(Collectors.toList());
        log.info("Seeking given partitions {} to beginning", tps);
        consumer.seekToBeginning(tps);
      }
    } else if (position == Position.CURRENT) {
      log.info("Seeking to given offsets {}", offsets);
      Utils.seekToOffsets(topic, offsets, consumer);
    } else {
      log.info("Starting to process kafka partitions from newest data");
    }
    return consumer;
  }

  @VisibleForTesting
  void validateTopic(KafkaConsumer<?, ?> consumer, String topicToValidate) {
    List<PartitionInfo> partitions = consumer.partitionsFor(topicToValidate);
    Preconditions.checkArgument(
        partitions != null && !partitions.isEmpty(),
        "Received null or empty partitions for topic [%s]. "
            + "Please check that the topic exists and has at least one partition.",
        topicToValidate);
  }

  @Override
  public boolean hasExternalizableOffsets() {
    return true;
  }

  @Override
  public Factory asFactory() {
    final KafkaAccessor accessor = this.accessor;
    final Context context = this.context;
    return repo -> new KafkaLogReader(accessor, context);
  }

  private static Collection<Offset> createDefaultOffsets(Collection<Partition> partitions) {
    if (partitions != null) {
      return partitions
          .stream()
          .map(p -> new TopicOffset(p.getId(), -1, Long.MIN_VALUE))
          .collect(Collectors.toList());
    }
    return null;
  }

  private static ObserveHandle dynamicHandle(AtomicReference<ObserveHandle> proxy) {
    return new ObserveHandle() {
      @Override
      public void close() {
        proxy.get().close();
      }

      @Override
      public List<Offset> getCommittedOffsets() {
        return proxy.get().getCommittedOffsets();
      }

      @Override
      public void resetOffsets(List<Offset> offsets) {
        proxy.get().resetOffsets(offsets);
      }

      @Override
      public List<Offset> getCurrentOffsets() {
        return proxy.get().getCurrentOffsets();
      }

      @Override
      public void waitUntilReady() throws InterruptedException {
        proxy.get().waitUntilReady();
      }
    };
  }

  private OffsetCommitter<TopicPartition> createOffsetCommitter() {
    return new OffsetCommitter<>(
        accessor.getLogStaleCommitIntervalNs(), accessor.getAutoCommitIntervalNs());
  }

  // create rebalance listener from consumer
  private ConsumerRebalanceListener listener(
      String name,
      AtomicReference<KafkaConsumer<Object, Object>> kafka,
      ElementConsumer<Object, Object> consumer,
      Map<Integer, Integer> emptyPollCount,
      AtomicReference<PartitionedWatermarkEstimator> watermarkEstimator) {

    return new ConsumerRebalanceListener() {

      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> parts) {
        // nop
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> parts) {
        List<Integer> partitions =
            parts.stream().map(TopicPartition::partition).sorted().collect(Collectors.toList());

        emptyPollCount.clear();
        for (Integer partition : partitions) {
          emptyPollCount.put(partition, 0);
        }

        if (partitions.isEmpty()) {
          watermarkEstimator.set(createWatermarkEstimatorForEmptyParts());
        } else {
          watermarkEstimator.set(
              new MinimalPartitionWatermarkEstimator(
                  partitions
                      .stream()
                      .collect(toMap(Functions.identity(), item -> createWatermarkEstimator()))));
        }

        Optional.ofNullable(kafka.get())
            .ifPresent(
                c ->
                    consumer.onAssign(
                        c,
                        name != null
                            ? getCommittedTopicOffsets(parts, c)
                            : getCurrentTopicOffsets(parts, c)));
      }

      List<TopicOffset> getCurrentTopicOffsets(
          Collection<TopicPartition> parts, KafkaConsumer<Object, Object> c) {
        return parts
            .stream()
            .map(
                tp ->
                    new TopicOffset(
                        tp.partition(), c.position(tp), watermarkEstimator.get().getWatermark()))
            .collect(Collectors.toList());
      }

      List<TopicOffset> getCommittedTopicOffsets(
          Collection<TopicPartition> parts, KafkaConsumer<Object, Object> c) {

        Map<TopicPartition, OffsetAndMetadata> committed =
            new HashMap<>(c.committed(new HashSet<>(parts)));
        for (TopicPartition tp : parts) {
          committed.putIfAbsent(tp, null);
        }
        return committed
            .entrySet()
            .stream()
            .map(
                entry -> {
                  final long offset = entry.getValue() == null ? 0L : entry.getValue().offset();
                  return new TopicOffset(
                      entry.getKey().partition(), offset, watermarkEstimator.get().getWatermark());
                })
            .collect(Collectors.toList());
      }

      private WatermarkEstimator createWatermarkEstimator() {
        final WatermarkIdlePolicyFactory idlePolicyFactory =
            accessor.getWatermarkConfiguration().getWatermarkIdlePolicyFactory();
        final WatermarkEstimatorFactory estimatorFactory =
            accessor.getWatermarkConfiguration().getWatermarkEstimatorFactory();
        return estimatorFactory.create(cfg, idlePolicyFactory);
      }
    };
  }

  private static PartitionedWatermarkEstimator createWatermarkEstimatorForEmptyParts() {
    return () -> Watermarks.MAX_WATERMARK;
  }
}
