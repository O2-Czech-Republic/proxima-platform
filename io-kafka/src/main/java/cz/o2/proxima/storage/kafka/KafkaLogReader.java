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
package cz.o2.proxima.storage.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Offset;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.kafka.Consumers.BulkConsumer;
import cz.o2.proxima.storage.kafka.Consumers.OnlineConsumer;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.o2.proxima.view.input.DataSourceUtils;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
public class KafkaLogReader extends AbstractStorage
    implements CommitLogReader, PartitionedView {

  @Getter
  final KafkaAccessor accessor;
  private final Context context;
  private final AtomicBoolean shutdown = new AtomicBoolean();
  private final long consumerPollInterval;
  private final long maxBytesPerSec;
  private final String topic;

  KafkaLogReader(KafkaAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getUri());
    this.accessor = accessor;
    this.context = context;
    this.consumerPollInterval = accessor.getConsumerPollInterval();
    this.maxBytesPerSec = accessor.getMaxBytesPerSec();
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

    return observeKafka(
        name, null, position, false,
        new KafkaLogObserver.LogObserverKafkaLogObserver(observer));
  }

  @Override
  public <T> Dataset<T> observe(
      Flow flow, String name, PartitionedLogObserver<T> observer) {

    BlockingQueue<T> queue = new ArrayBlockingQueue<>(100);
    AtomicReference<ObserveHandle> handle = new AtomicReference<>();

    DataSourceUtils.Producer producer = () ->
        handle.set(observeKafka(name, null, Position.NEWEST, false,
            KafkaLogObserver.PartitionedKafkaLogObserver.of(
                observer, Utils.unchecked(queue::put))));

    Serializable lock = new Serializable() { };
    DataSource<T> source = DataSourceUtils.fromPartitions(
        DataSourceUtils.fromBlockingQueue(
            queue, producer, () -> handle.get().getCurrentOffsets(),
            off -> {
              synchronized (lock) {
                Optional.ofNullable(handle.get()).ifPresent(h -> h.resetOffsets(off));
              }
            }));

    // we need to remap the input here to be able to directly persist it again
    return MapElements.of(
        flow.createInput(source))
        .using(e -> e)
        .output();
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
        new KafkaLogObserver.LogObserverKafkaLogObserver(observer));

  }

  @Override
  public <T> Dataset<T> observePartitions(
      Flow flow, Collection<Partition> partitions,
      PartitionedLogObserver<T> observer) {

    BlockingQueue<T> queue = new ArrayBlockingQueue<>(100);
    AtomicReference<ObserveHandle> handle = new AtomicReference<>();

    DataSourceUtils.Producer producer = () ->
        handle.set(observeKafka(
            null, partitions, Position.NEWEST, false,
            KafkaLogObserver.PartitionedKafkaLogObserver.of(
                observer, Utils.unchecked(queue::put))));

    final Serializable lock = new Serializable() { };
    DataSource<T> source = DataSourceUtils.fromPartitions(
        DataSourceUtils.fromBlockingQueue(
            queue, producer, () -> handle.get().getCurrentOffsets(),
            off -> {
              synchronized (lock) {
                Optional.ofNullable(handle.get()).ifPresent(h -> h.resetOffsets(off));
              }
            }));

    // we need to remap the input here to be able to directly persist it again
    return MapElements.of(
        flow.createInput(source))
        .using(e -> e)
        .output();
  }

  @Override
  public ObserveHandle observeBulk(
      String name,
      Position position,
      boolean stopAtCurrent,
      BulkLogObserver observer) {

    return observeKafkaBulk(name, null, position, stopAtCurrent, observer);
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      BulkLogObserver observer) {

    // name is ignored, because when observing partition the offsets
    // are not committed to kafka
    return observeKafkaBulk(
        null, asOffsets(partitions),
        position, stopAtCurrent,
        observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(
      Collection<Offset> offsets, BulkLogObserver observer) {

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
      KafkaLogObserver observer) {

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
      BulkLogObserver observer) {

    Preconditions.checkArgument(
        name != null || offsets != null,
        "Either name of offsets have to be non null");

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
      KafkaLogObserver observer,
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
        preWrite, offsetCommitter::clear, onlineConsumer, executor, handle);
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
      BulkLogObserver observer,
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
        });

    AtomicReference<ObserveHandle> handle = new AtomicReference<>();
    submitConsumerWithObserver(
        name, offsets, position, stopAtCurrent, (tp, r) -> { },
        kafkaCommitMap::clear,
        bulkConsumer, executor, handle);
    return dynamicHandle(handle);
  }

  private void submitConsumerWithObserver(
      @Nullable String name,
      @Nullable Collection<Offset> offsets,
      Position position,
      boolean stopAtCurrent,
      BiConsumer<TopicPartition,
      ConsumerRecord<String, byte[]>> preWrite,
      Runnable preStart,
      ElementConsumer consumer,
      ExecutorService executor,
      AtomicReference<ObserveHandle> handle) throws InterruptedException {

    final CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean completed = new AtomicBoolean();
    List<TopicOffset> seekOffsets = Collections.synchronizedList(new ArrayList<>());

    executor.submit(() -> {
      handle.set(new ObserveHandle() {

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

      });
      final AtomicReference<KafkaConsumer<String, byte[]>> consumerRef;
      consumerRef = new AtomicReference<>();
      preStart.run();
      try (KafkaConsumer<String, byte[]> kafka = createConsumer(
          name, offsets, listener(name, consumerRef, consumer), position)) {

        consumerRef.set(kafka);

        final Map<TopicPartition, Long> endOffsets;
        if (stopAtCurrent) {
          Set<TopicPartition> assignment = kafka.assignment();
          Map<TopicPartition, Long> beginning;

          beginning = kafka.beginningOffsets(assignment);
          endOffsets = kafka.endOffsets(assignment)
              .entrySet()
              .stream()
              .filter(entry -> beginning.get(entry.getKey()) < entry.getValue())
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } else {
          endOffsets = null;
        }

        // we need to poll first to initialize kafka assignments and
        // rebalance listener
        if (offsets != null) {
          // when manual offsets are assigned, we need to ensure calling
          // onAssign by hand
          consumer.onAssign(kafka, kafka.assignment()
              .stream()
              .map(tp -> {
                final long offset;
                if (name != null) {
                  offset = Optional.ofNullable(kafka.committed(tp))
                    .map(OffsetAndMetadata::offset)
                    .orElse(0L);
                } else {
                  offset = kafka.position(tp);
                }
                return new TopicOffset(tp.partition(), offset);
              })
              .collect(Collectors.toList()));
        }

        ConsumerRecords<String, byte[]> poll = kafka.poll(consumerPollInterval);

        latch.countDown();

        AtomicReference<Throwable> error = new AtomicReference<>();
        do {
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
          for (ConsumerRecord<String, byte[]> r : poll) {
            bytesPolled += r.serializedKeySize() + r.serializedValueSize();
            String key = r.key();
            byte[] value = r.value();
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());
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
              }
            }
            boolean cont = consumer.consumeWithConfirm(
                ingest, tp, r.offset(), error::set);
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
          Map<TopicPartition, OffsetAndMetadata> commitMapClone;
          commitMapClone = consumer.prepareOffsetsForCommit();
          if (!commitMapClone.isEmpty()) {
            kafka.commitSync(commitMapClone);
          }
          if (stopAtCurrent && endOffsets.isEmpty()) {
            log.info("Reached end of current data. Terminating consumption.");
            completed.set(true);
          }
          Throwable errorThrown = error.getAndSet(null);
          if (errorThrown != null) {
            throw new RuntimeException(errorThrown);
          }
          long sleepDuration = bytesPolled * consumerPollInterval / bytesPerPoll;
          if (sleepDuration > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepDuration);
          }
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
                preWrite, preStart, consumer, executor, handle);
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
        log.info("Seeking consumer name {} to beginning of partitions", name);
        consumer.seekToBeginning(consumer.assignment());
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
      String name,
      AtomicReference<KafkaConsumer<String, byte[]>> kafka,
      ElementConsumer consumer) {

    if (name == null) {
      return null;
    }
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> parts) {
        // nop
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> parts) {
        Optional.ofNullable(kafka.get()).ifPresent(c ->
            consumer.onAssign(c, c.assignment().stream()
                .map(tp -> new TopicOffset(tp.partition(), c.position(tp)))
                .collect(Collectors.toList())));
      }
    };
  }

}
