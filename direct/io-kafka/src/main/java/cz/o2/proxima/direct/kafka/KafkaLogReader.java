/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.commitlog.Position;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.direct.kafka.Consumers.BulkConsumer;
import cz.o2.proxima.direct.kafka.Consumers.OnlineConsumer;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.time.VectorClock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

/**
 * A {@link CommitLogReader} implementation for Kafka.
 */
@Slf4j
public class KafkaLogReader extends AbstractStorage implements CommitLogReader {

  @Getter
  final KafkaAccessor accessor;
  private final Context context;
  private final AtomicBoolean shutdown = new AtomicBoolean();
  private final long consumerPollInterval;
  private final long maxBytesPerSec;
  private final long timestampSkew;
  private final int emptyPolls;
  private final int maxPollRecords;
  private final String topic;

  KafkaLogReader(KafkaAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getUri());
    this.accessor = accessor;
    this.context = context;
    this.consumerPollInterval = accessor.getConsumerPollInterval();
    this.maxBytesPerSec = accessor.getMaxBytesPerSec();
    this.timestampSkew = accessor.getTimestampSkew();
    this.emptyPolls = accessor.getEmptyPolls();
    this.maxPollRecords = accessor.getMaxPollRecords();
    this.topic = accessor.getTopic();
  }

  /**
   * Subscribe observer by name to the commit log.
   * Each observer maintains its own position in the commit log, so that
   * the observers with different names do not interfere
   * If multiple observers share the same name, then the ingests
   * are load-balanced between them (in an undefined manner).
   * This is a non blocking call.
   * @param name identifier of the consumer
   */
  @Override
  public ObserveHandle observe(
      String name,
      Position position,
      LogObserver observer) {

    return observeKafka(name, null, position, false, observer);
  }

  @Override
  public ObserveHandle observePartitions(
      String name,
      @Nullable Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    return observeKafka(
        null, partitions, position, stopAtCurrent,
        observer);

  }

  @Override
  public ObserveHandle observeBulk(
      String name,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

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
        null, asOffsets(partitions),
        position, stopAtCurrent,
        observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(
      Collection<Offset> offsets, LogObserver observer) {

    return observeKafkaBulk(null, offsets, Position.CURRENT, false, observer);
  }

  @Override
  public List<Partition> getPartitions() {
    final List<PartitionInfo> partitions;
    try (KafkaConsumer<String, byte[]> consumer = createConsumer()) {
      partitions = consumer.partitionsFor(topic);
    }
    return partitions.stream()
        .map(p -> {
          final int id = p.partition();
          return (Partition) (() -> id);
        })
        .collect(Collectors.toList());
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
          name, asOffsets(partitions), position, stopAtCurrent,
          name != null, observer, context.getExecutorService());
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
        name != null || offsets != null,
        "Either name or offsets have to be non null");

    Preconditions.checkArgument(
        position != null,
        "Position cannot be null");

    try {
      return processConsumerBulk(
          name, offsets, position, stopAtCurrent,
          name != null, observer, context.getExecutorService());
    } catch (InterruptedException ex) {
      log.warn("Interrupted waiting for kafka observer to start", ex);
      Thread.currentThread().interrupt();
      throw new RuntimeException(ex);
    }

  }

  /**
   * Process given consumer in online fashion.
   * @param name name of the consumer
   * @param offsets assigned offsets
   * @param position where to read from
   * @param stopAtCurrent termination flag
   * @param listener the rebalance listener
   * @param commitToKafka should we commit to kafka
   * @param observer the observer
   * @param executor executor to use for async processing
   * @return observe handle
   */
  @VisibleForTesting
  ObserveHandle processConsumer(
      @Nullable String name, @Nullable Collection<Offset> offsets,
      Position position, boolean stopAtCurrent,
      boolean commitToKafka,
      LogObserver observer,
      ExecutorService executor) throws InterruptedException {

    // offsets that should be committed to kafka
    Map<TopicPartition, OffsetAndMetadata> kafkaCommitMap;
    kafkaCommitMap = Collections.synchronizedMap(new HashMap<>());

    final OffsetCommitter<TopicPartition> offsetCommitter = new OffsetCommitter<>();

    BiConsumer<TopicPartition, ConsumerRecord<String, byte[]>> preWrite = (tp, r) ->
        offsetCommitter.register(tp, r.offset(), 1,
            () -> {
              OffsetAndMetadata mtd = new OffsetAndMetadata(r.offset() + 1);
              if (commitToKafka) {
                kafkaCommitMap.put(tp, mtd);
              }
            });

    OnlineConsumer onlineConsumer = new OnlineConsumer(
        observer, offsetCommitter,
        () -> {
          synchronized (kafkaCommitMap) {
            Map<TopicPartition, OffsetAndMetadata> clone = new HashMap<>(kafkaCommitMap);
            kafkaCommitMap.clear();
            return clone;
          }
        });

    AtomicReference<ObserveHandle> handle = new AtomicReference<>();
    submitConsumerWithObserver(
        name, offsets, position, stopAtCurrent,
        preWrite, onlineConsumer, executor, handle);
    return dynamicHandle(handle);
  }


  /**
   * Process given consumer in bulk fashion.
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
      @Nullable String name, @Nullable Collection<Offset> offsets,
      Position position, boolean stopAtCurrent,
      boolean commitToKafka,
      LogObserver observer,
      ExecutorService executor) throws InterruptedException {

    // offsets that should be committed to kafka
    Map<TopicPartition, OffsetAndMetadata> kafkaCommitMap;
    kafkaCommitMap = Collections.synchronizedMap(new HashMap<>());

    final BulkConsumer bulkConsumer = new BulkConsumer(
        topic, observer,
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
        name, offsets, position, stopAtCurrent, (tp, r) -> { },
        bulkConsumer, executor, handle);
    return dynamicHandle(handle);
  }

  private void submitConsumerWithObserver(
      final @Nullable String name,
      final @Nullable Collection<Offset> offsets,
      final Position position,
      boolean stopAtCurrent,
      final BiConsumer<TopicPartition, ConsumerRecord<String, byte[]>> preWrite,
      final ElementConsumer consumer,
      final ExecutorService executor,
      final AtomicReference<ObserveHandle> handle) throws InterruptedException {

    final CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean completed = new AtomicBoolean();
    List<TopicOffset> seekOffsets = Collections.synchronizedList(new ArrayList<>());

    executor.submit(() -> {
      handle.set(createObserveHandle(completed, seekOffsets, consumer, latch));
      final AtomicReference<KafkaConsumer<String, byte[]>> consumerRef;
      final AtomicReference<VectorClock> clock = new AtomicReference<>();
      final Map<Integer, Integer> partitionToClockDimension = new ConcurrentHashMap<>();
      final Map<Integer, Integer> emptyPollCount = new ConcurrentHashMap<>();
      consumerRef = new AtomicReference<>();
      consumer.onStart();
      ConsumerRebalanceListener listener = listener(
          name, consumerRef, consumer, clock, partitionToClockDimension,
          emptyPollCount);

      try (KafkaConsumer<String, byte[]> kafka = createConsumer(
          name, offsets, name != null ? listener : null, position)) {

        consumerRef.set(kafka);

        Map<TopicPartition, Long> endOffsets = stopAtCurrent
            ? findNonEmptyEndOffsets(kafka) : null;

        // we need to poll first to initialize kafka assignments and
        // rebalance listener
        ConsumerRecords<String, byte[]> poll = kafka.poll(consumerPollInterval);

        if (offsets != null) {
          // when manual offsets are assigned, we need to ensure calling
          // onAssign by hand
          listener.onPartitionsAssigned(kafka.assignment());
        }

        latch.countDown();

        AtomicReference<Throwable> error = new AtomicReference<>();
        int nonEmptyNotFullPolled = 0;
        do {
          if (poll.count() < maxPollRecords) {
            if (!poll.isEmpty()) {
              nonEmptyNotFullPolled++;
            }
          } else {
            nonEmptyNotFullPolled = 0;
          }
          if (log.isDebugEnabled()) {
            log.debug(
                "Current watermark of consumer name {} with offsets {} "
                    + "on {} poll'd records is {}",
                name, offsets, poll.count(), clock.get().getStamp());
          }
          synchronized (seekOffsets) {
            if (!seekOffsets.isEmpty()) {
              Utils.seekToOffsets(topic, offsets, kafka);
              consumer.onAssign(kafka, kafka.assignment().stream()
                  .map(tp -> new TopicOffset(tp.partition(), kafka.position(tp)))
                  .collect(Collectors.toList()));
              log.info("Seeked consumer to offsets {} as requested", seekOffsets);
              seekOffsets.clear();
              poll = ConsumerRecords.empty();
            }
          }
          final long bytesPerPoll = maxBytesPerSec < Long.MAX_VALUE
              ? Math.max(1L, maxBytesPerSec / (1000L * consumerPollInterval))
              : Long.MAX_VALUE;
          long bytesPolled = 0L;
          if (nonEmptyNotFullPolled > 0 && poll.isEmpty()) {
            // increase all partition's empty poll counter by 1
            emptyPollCount.replaceAll((k, v) -> v + 1);
          }
          for (ConsumerRecord<String, byte[]> r : poll) {
            bytesPolled += r.serializedKeySize() + r.serializedValueSize();
            String key = r.key();
            byte[] value = r.value();
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());
            emptyPollCount.put(tp.partition(), 0);
            preWrite.accept(tp, r);
            // in kafka, each entity attribute is separated by `#' from entity key
            int hashPos = key.lastIndexOf('#');
            KafkaStreamElement ingest = null;
            if (hashPos < 0 || hashPos >= key.length()) {
              log.error("Invalid key in kafka topic: {}", key);
            } else {
              String entityKey = key.substring(0, hashPos);
              String attribute = key.substring(hashPos + 1);
              Optional<AttributeDescriptor<Object>> attr = getEntityDescriptor()
                    .findAttribute(attribute, true /* allow reading protected */);
              if (!attr.isPresent()) {
                log.error("Invalid attribute {} in kafka key {}", attribute, key);
              } else {
                ingest = new KafkaStreamElement(
                    getEntityDescriptor(), attr.get(),
                    String.valueOf(r.topic() + "#" + r.partition() + "#" + r.offset()),
                    entityKey, attribute, r.timestamp(), value, r.partition(),
                    r.offset());
                // move watermark
                clock.get().update(
                    partitionToClockDimension.get(tp.partition()), ingest.getStamp());
              }
            }
            boolean cont = consumer.consumeWithConfirm(
                ingest, tp, r.offset(), clock.get(), error::set);
            if (!cont) {
              log.info("Terminating consumption by request");
              completed.set(true);
              break;
            }
            if (stopAtCurrent) {
              Long end = endOffsets.get(tp);
              if (end != null && end - 1 <= r.offset()) {
                endOffsets.remove(tp);
              }
            }
          }
          if (nonEmptyNotFullPolled > 0) {
            increaseWatermarkOnEmptyPolls(
                emptyPollCount, partitionToClockDimension, clock);
          }
          flushCommits(kafka, consumer);
          rethrowErrorIfPresent(error);
          terminateIfConsumed(stopAtCurrent, endOffsets, completed);
          waitToReduceThroughput(bytesPolled, bytesPerPoll);
          poll = kafka.poll(consumerPollInterval);
        } while (!shutdown.get() && !completed.get()
            && !Thread.currentThread().isInterrupted());

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
                name, offsets, position, stopAtCurrent,
                preWrite, consumer, executor, handle);
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

  private void rethrowErrorIfPresent(AtomicReference<Throwable> error) {
    Throwable errorThrown = error.getAndSet(null);
    if (errorThrown != null) {
      throw new RuntimeException(errorThrown);
    }
  }

  private void terminateIfConsumed(
      boolean stopAtCurrent,
      Map<TopicPartition, Long> endOffsets,
      AtomicBoolean completed) {

    if (stopAtCurrent && endOffsets.isEmpty()) {
      log.info("Reached end of current data. Terminating consumption.");
      completed.set(true);
    }
  }

  private void waitToReduceThroughput(
      long bytesPolled, final long bytesPerPoll)
      throws InterruptedException {

    long sleepDuration = bytesPolled * consumerPollInterval / bytesPerPoll;
    if (sleepDuration > 0) {
      TimeUnit.MILLISECONDS.sleep(sleepDuration);
    }
  }

  private void flushCommits(
      final KafkaConsumer<String, byte[]> kafka,
      ElementConsumer consumer) {

    Map<TopicPartition, OffsetAndMetadata> commitMapClone;
    commitMapClone = consumer.prepareOffsetsForCommit();
    if (!commitMapClone.isEmpty()) {
      kafka.commitSync(commitMapClone);
    }
  }

  private void increaseWatermarkOnEmptyPolls(
      Map<Integer, Integer> emptyPollCount,
      Map<Integer, Integer> partitionToClockDimension,
      AtomicReference<VectorClock> clock) {

    long nowSkewed = System.currentTimeMillis() - timestampSkew;
    emptyPollCount.entrySet()
        .stream()
        .filter(e -> e.getValue() >= emptyPolls)
        .forEach(e ->
            clock.get().update(partitionToClockDimension.get(e.getKey()), nowSkewed));
  }

  private ObserveHandle createObserveHandle(
      AtomicBoolean completed, List<TopicOffset> seekOffsets,
      ElementConsumer consumer, CountDownLatch latch) {

    return new ObserveHandle() {

      @Override
      public void cancel() {
        completed.set(true);
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
      final KafkaConsumer<String, byte[]> kafka) {

    Set<TopicPartition> assignment = kafka.assignment();
    Map<TopicPartition, Long> beginning;

    beginning = kafka.beginningOffsets(assignment);
    return kafka.endOffsets(assignment)
        .entrySet()
        .stream()
        .filter(entry -> beginning.get(entry.getKey()) < entry.getValue())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }


  private KafkaConsumer<String, byte[]> createConsumer() {
    return createConsumer(
        UUID.randomUUID().toString(), null, null, Position.NEWEST);
  }

  /** Create kafka consumer for the data. */
  @SuppressWarnings("unchecked")
  private KafkaConsumer<String, byte[]> createConsumer(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      @Nullable ConsumerRebalanceListener listener,
      Position position) {

    Preconditions.checkArgument(
        name != null || listener == null,
        "Please use either named group (with listener) or offsets without listener");
    KafkaConsumerFactory factory = accessor.createConsumerFactory();
    final KafkaConsumer<String, byte[]> consumer;

    if ("".equals(name)) {
      throw new IllegalArgumentException("Consumer group cannot be empty string");
    }
    if (name != null) {
      consumer = factory.create(name, listener);
    } else if (offsets != null) {
      List<Partition> partitions = offsets.stream()
          .map(Offset::getPartition)
          .collect(Collectors.toList());
      consumer = factory.create(partitions);
    } else {
      throw new IllegalArgumentException("Need either name or offsets to observe");
    }
    if (position == Position.OLDEST) {
      // seek all partitions to oldest data
      if (offsets == null) {
        Set<TopicPartition> assignment = consumer.assignment();
        log.info(
            "Seeking consumer name {} to beginning of partitions {}",
            name, assignment);
        consumer.seekToBeginning(assignment);
      } else {
        List<TopicPartition> tps = offsets.stream()
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

  @Override
  public void close() {
    this.shutdown.set(true);
  }

  private static Collection<Offset> asOffsets(Collection<Partition> partitions) {
    if (partitions != null) {
      return partitions.stream()
          .map(p -> new TopicOffset(p.getId(), -1))
          .collect(Collectors.toList());
    }
    return null;
  }

  private static ObserveHandle dynamicHandle(AtomicReference<ObserveHandle> proxy) {
    return new ObserveHandle() {
      @Override
      public void cancel() {
        proxy.get().cancel();
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

  // create rebalance listener from consumer
  private ConsumerRebalanceListener listener(
      String name, AtomicReference<KafkaConsumer<String, byte[]>> kafka,
      ElementConsumer consumer, AtomicReference<VectorClock> clock,
      Map<Integer, Integer> partitionToClockDimension,
      Map<Integer, Integer> emptyPollCount) {

    return new ConsumerRebalanceListener() {

      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> parts) {
        // nop
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> parts) {
        List<Integer> partitions = parts.stream()
            .map(TopicPartition::partition)
            .sorted()
            .collect(Collectors.toList());

        emptyPollCount.clear();
        for (int pos = 0; pos < partitions.size(); pos++) {
          partitionToClockDimension.put(partitions.get(pos), pos);
          emptyPollCount.put(partitions.get(pos), 0);
        }
        clock.set(VectorClock.of(parts.size()));

        Optional.ofNullable(kafka.get()).ifPresent(c ->
            consumer.onAssign(c, parts.stream().map(tp -> {
              final long offset;
              if (name != null) {
                offset = Optional.ofNullable(c.committed(tp))
                  .map(OffsetAndMetadata::offset)
                  .orElse(0L);
              } else {
                offset = c.position(tp);
              }
              return new TopicOffset(tp.partition(), offset);
            }).collect(Collectors.toList())));
      }
    };
  }

}
