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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.Cancellable;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.LogObserverBase;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.o2.proxima.view.input.DataSourceUtils;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
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
  private final String topic;

  KafkaLogReader(KafkaAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getURI());
    this.accessor = accessor;
    this.context = context;
    this.consumerPollInterval = accessor.getConsumerPollInterval();
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
  public Cancellable observe(
      String name,
      Position position,
      LogObserver observer) {

    return observePartitions(name, null, position, false, observer, null);
  }


  @Override
  public Cancellable observePartitions(
      @Nullable Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    return observePartitions(
        null, partitions, position, stopAtCurrent, observer, null);
  }

  @Override
  public Cancellable observeBulk(
      String name,
      Position position,
      BulkLogObserver observer) {

    return observePartitionsBulk(name, position, observer);
  }

  @Override
  public <T> Dataset<T> observePartitions(
      Flow flow, Collection<Partition> partitions,
      PartitionedLogObserver<T> observer) {

    BlockingQueue<T> queue = new ArrayBlockingQueue<>(100);

    DataSourceUtils.Producer producer = () -> {
      observePartitions(null, partitions, Position.NEWEST, false,
          Utils.forwardingTo(queue, observer),
          Utils.rebalanceListener(observer));
    };


    // FIXME: revisit this logic, we need to be able to correctly commit
    // and rewind the source (https://github.com/O2-Czech-Republic/proxima-platform/issues/57)
    DataSource<T> source = DataSourceUtils.fromPartitions(
        DataSourceUtils.fromBlockingQueue(queue, producer, () -> 0, a -> null, a -> null));

    // we need to remap the input here to be able to directly persist it again
    return MapElements.of(
        flow.createInput(source))
        .using(e -> e)
        .output();
  }

  @Override
  public <T> Dataset<T> observe(
      Flow flow, String name, PartitionedLogObserver<T> observer) {

    BlockingQueue<T> queue = new ArrayBlockingQueue<>(100);

    DataSourceUtils.Producer producer = () -> {
      observePartitions(name, null, Position.NEWEST, false,
          Utils.forwardingTo(queue, observer),
          Utils.rebalanceListener(observer));
    };

    // FIXME: revisit this logic, we need to be able to correctly commit
    // and rewind the source (https://github.com/O2-Czech-Republic/proxima-platform/issues/57)
    DataSource<T> source = DataSourceUtils.fromPartitions(
        DataSourceUtils.fromBlockingQueue(
            queue, producer, () -> 0, a -> null, a -> null));

    // we need to remap the input here to be able to directly persist it again
    return MapElements.of(
        flow.createInput(source))
        .using(e -> e)
        .output();
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

  protected Cancellable observePartitions(
      @Nullable String name,
      @Nullable Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer,
      @Nullable ConsumerRebalanceListener listener) {

    // wait until the consumer is really created
    CountDownLatch latch = new CountDownLatch(1);

    ExecutorService executor = context.getExecutorService();

    // start new thread that will fill our observer
    AtomicReference<Future<?>> submit = new AtomicReference<>();
    runConsumption(
        name, partitions, position, stopAtCurrent,
        listener, submit, executor, latch, observer);

    try {
      log.debug("Waiting for the consumer {} to be created and run", name);
      latch.await();
    } catch (InterruptedException ex) {
      log.warn("Interrupted while waiting for the creation of the consumer.", ex);
      Thread.currentThread().interrupt();
    }

    return () -> {
      submit.get().cancel(true);
    };
  }

  private void runConsumption(
      String name, Collection<Partition> partitions,
      Position position, boolean stopAtCurrent,
      ConsumerRebalanceListener listener,
      AtomicReference<Future<?>> submit, ExecutorService executor,
      @Nullable CountDownLatch latch, LogObserver observer) {

    submit.set(executor.submit(() -> {
      try {
        try (KafkaConsumer<String, byte[]> kafkaConsumer = createConsumer(
            name, partitions, listener, position)) {
          if (partitions != null) {
            List<TopicPartition> assignment = partitions.stream()
                .map(p -> new TopicPartition(topic, p.getId()))
                .collect(Collectors.toList());
            kafkaConsumer.assign(assignment);
          }
          if (latch != null) {
            latch.countDown();
          }
          processConsumer(
              kafkaConsumer,
              name != null,
              name != null ? false : stopAtCurrent,
              observer);
        }
      } catch (Throwable thwbl) {
        log.error("Error in running the observer {}", name, thwbl);
        if (observer.onError(thwbl)) {
          log.info("Restarting consumption as requested");
          runConsumption(
              name, partitions, position, stopAtCurrent,
              listener, submit, executor, null, observer);
        }
      }
    }));
  }

  private Cancellable observePartitionsBulk(
      String name, Position position, BulkLogObserver observer) {

    Objects.requireNonNull(
        "You can bulk observe only with named observers!",
        name);
    final AtomicReference<KafkaConsumer<String, byte[]>> consumerRef;
    consumerRef = new AtomicReference<>();

    ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // nop
      }

      @Override
      public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        try {
          observer.onRestart();
          if (consumerRef.get() != null) {
            for (TopicPartition tp : partitions) {
              OffsetAndMetadata off = consumerRef.get().committed(tp);
              if (off != null) {
                log.info(
                    "Seeking to offset {} for consumer name {} on partition {}",
                    off.offset(), name, tp);
                consumerRef.get().seek(tp, off.offset());
              } else {
                log.debug(
                    "Partition {} for consumer name {} has no committed offset",
                    tp, name);
              }
            }
          }
        } catch (Exception | Error err) {
          log.error(
              "Failed to seek to committed offsets for {}",
              partitions, err);
          throw new RuntimeException(err);
        }
      }

    };

    // wait until the consumer is really created
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Future<?>> submit = new AtomicReference<>();

    runBulkConsumption(
        name, position, listener,
        submit, consumerRef, latch, observer);

    try {
      log.debug("Waiting for the consumer {} to be created and run", name);
      latch.await();
    } catch (InterruptedException ex) {
      log.warn("Interrupted while waiting for the creation of the consumer.", ex);
      Thread.currentThread().interrupt();
    }
    return () -> submit.get().cancel(true);
  }

  private void runBulkConsumption(
      String name, Position position, ConsumerRebalanceListener listener,
      AtomicReference<Future<?>> submit,
      AtomicReference<KafkaConsumer<String, byte[]>> consumerRef,
      @Nullable CountDownLatch latch, BulkLogObserver observer) {

    // start new thread that will fill our observer
    submit.set(context.getExecutorService().submit(() -> {

      consumerRef.set(createConsumer(name, null, listener, position));
      try {
        if (latch != null) {
          latch.countDown();
        }
        processConsumer(consumerRef.get(), true, false, observer);
        consumerRef.get().close();
      } catch (Exception | Error exc) {
        log.error("Exception in running the observer {}", name, exc);
        consumerRef.get().close();
        if (observer.onError(exc)) {
          log.info("Restarting consumption as requested");
          runBulkConsumption(
              name, position, listener, submit,
              consumerRef, null, observer);
        } else {
          log.info("Terminating consumption as requested.");
        }
      }
    }));
  }



  private void processConsumer(
      KafkaConsumer<String, byte[]> kafkaConsumer,
      boolean named,
      boolean stopAtCurrent,
      LogObserver observer) {

    Map<TopicPartition, OffsetAndMetadata> commitMap = Collections.synchronizedMap(
        new HashMap<>());
    OffsetCommitter<TopicPartition> offsetCommitter = new OffsetCommitter<>();

    BiFunction<TopicPartition, ConsumerRecord<String, byte[]>, Void> preWrite = (tp, r) -> {
        if (named) {
          offsetCommitter.register(tp, r.offset(), 1,
              () -> commitMap.put(tp, new OffsetAndMetadata(r.offset() + 1)));
        }
        return null;
    };

    KafkaAccessor.OnlineConsumer onlineConsumer = new KafkaAccessor.OnlineConsumer(observer, (tp, offset) -> {
        if (named) {
          offsetCommitter.confirm(tp, offset);
        }
    });

    processConsumerWithObserver(
        kafkaConsumer, named,
        stopAtCurrent, preWrite,
        onlineConsumer,
        commitMap,
        observer);
  }


  private void processConsumer(
      KafkaConsumer<String, byte[]> kafkaConsumer,
      boolean named,
      boolean stopAtCurrent,
      BulkLogObserver observer) {

    Map<TopicPartition, OffsetAndMetadata> commitMap = Collections.synchronizedMap(
        new HashMap<>());
    Map<TopicPartition, OffsetAndMetadata> uncommittedMap = Collections.synchronizedMap(
        new HashMap<>());

    BiFunction<TopicPartition, ConsumerRecord<String, byte[]>, Void> preWrite = (tp, r) -> {
      if (named) {
        uncommittedMap.put(tp, new OffsetAndMetadata(r.offset() + 1));
      }
      return null;
    };

    KafkaAccessor.BulkConsumer bulkConsumer = new KafkaAccessor.BulkConsumer(observer, (tp, offset) -> {
      if (named) {
        Map<TopicPartition, OffsetAndMetadata> copy;
        synchronized (uncommittedMap) {
          copy = new HashMap<>(uncommittedMap);
          uncommittedMap.clear();
        }
        synchronized (commitMap) {
          commitMap.putAll(copy);
        }
      }
    });

    processConsumerWithObserver(
        kafkaConsumer, named,
        stopAtCurrent, preWrite,
        bulkConsumer,
        commitMap,
        observer);
  }



  private void processConsumerWithObserver(
      KafkaConsumer<String, byte[]> kafkaConsumer,
      boolean named,
      boolean stopAtCurrent,
      BiFunction<TopicPartition, ConsumerRecord<String, byte[]>, Void> preWrite,
      KafkaAccessor.ElementConsumer consumer,
      Map<TopicPartition, OffsetAndMetadata> commitMap,
      LogObserverBase baseObserver) {

    final Map<TopicPartition, Long> endOffsets;
    if (stopAtCurrent) {
      Set<TopicPartition> assignment = kafkaConsumer.assignment();
      Map<TopicPartition, Long> beginning;

      beginning = kafkaConsumer.beginningOffsets(assignment);
      endOffsets = kafkaConsumer.endOffsets(assignment)
          .entrySet()
          .stream()
          .filter(entry -> beginning.get(entry.getKey()) < entry.getValue())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    } else {
      endOffsets = null;
    }

    boolean completed = false;

    AtomicReference<Throwable> error = new AtomicReference<>();
    while (!shutdown.get() && !completed && !Thread.currentThread().isInterrupted()) {
      ConsumerRecords<String, byte[]> poll = kafkaConsumer.poll(consumerPollInterval);
      poll.forEach(r -> {
        String key = r.key();
        byte[] value = r.value();
        TopicPartition tp = new TopicPartition(r.topic(), r.partition());
        preWrite.apply(tp, r);
        // in kafka, each entity attribute is separated by `#' from entity key
        int hashPos = key.lastIndexOf("#");
        KafkaStreamElement ingest = null;
        if (hashPos < 0 || hashPos >= key.length()) {
          log.error("Invalid key in kafka topic: {}", key);
        } else {
          String entityKey = key.substring(0, hashPos);
          String attribute = key.substring(hashPos + 1);
          Optional<AttributeDescriptor<?>> attr = getEntityDescriptor().findAttribute(attribute);
          if (!attr.isPresent()) {
            log.error("Invalid attribute in kafka key {}", key);
          } else {
            ingest = new KafkaStreamElement(
                getEntityDescriptor(), attr.get(),
                String.valueOf(r.topic() + "#" + r.partition() + "#" + r.offset()),
                entityKey, attribute, r.timestamp(), value, r.partition(), r.offset());
          }
        }
        consumer.consumeWithConfirm(
            ingest, tp, r.offset(), exc -> error.set(exc));
        if (endOffsets != null) {
          Long offset = endOffsets.get(tp);
          if (offset != null && offset <= r.offset() + 1) {
            endOffsets.remove(tp);
          }
        }
      });
      Map<TopicPartition, OffsetAndMetadata> commitMapClone = null;
      synchronized (commitMap) {
        if (!commitMap.isEmpty()) {
          commitMapClone = new HashMap<>(commitMap);
          commitMap.clear();
        }
      }
      if (named && commitMapClone != null) {
        kafkaConsumer.commitSync(commitMapClone);
      }
      if (stopAtCurrent && endOffsets.isEmpty()) {
        log.info("Reached end of current data. Terminating consumption.");
        completed = true;
      }
      Throwable errorThrown = error.getAndSet(null);
      if (errorThrown != null) {
        throw new RuntimeException(errorThrown);
      }
    }
    if (!Thread.currentThread().isInterrupted()) {
      baseObserver.onCompleted();
    } else {
      baseObserver.onCancelled();
    }
  }


  private KafkaConsumer<String, byte[]> createConsumer() {
    return createConsumer(
        "dummy-consumer",
        null, null, Position.NEWEST);
  }

  /** Create kafka consumer for the data. */
  @SuppressWarnings("unchecked")
  private KafkaConsumer<String, byte[]> createConsumer(
      @Nullable String name,
      @Nullable Collection<Partition> partitions,
      @Nullable ConsumerRebalanceListener listener,
      Position position) {

    KafkaConsumerFactory factory = accessor.createConsumerFactory();
    final KafkaConsumer<String, byte[]> consumer;

    if (name != null) {
      consumer = factory.create(name, listener);
    } else if (partitions != null) {
      if (listener != null) {
        listener.onPartitionsAssigned(
            partitions.stream()
                .map(p -> new TopicPartition(topic, p.getId()))
                .collect(Collectors.toList()));
      }
      consumer = factory.create(partitions);
    } else {
      throw new IllegalArgumentException("Need either name or partitions to observe");
    }
    if (position == Position.OLDEST) {
      if (partitions == null) {
        consumer.seekToBeginning(consumer.assignment());
      } else {
        consumer.seekToBeginning(partitions.stream()
            .map(p -> new TopicPartition(topic, p.getId()))
            .collect(Collectors.toList()));
      }
    }
    return consumer;
  }

  @Override
  public void close() {
    this.shutdown.set(true);
  }

}
