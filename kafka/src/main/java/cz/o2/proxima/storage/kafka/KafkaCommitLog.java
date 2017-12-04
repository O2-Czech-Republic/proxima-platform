/**
 * Copyright 2017 O2 Czech Republic, a.s.
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
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractOnlineAttributeWriter;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.LogObserverBase;
import cz.o2.proxima.storage.kafka.partitioner.KeyPartitioner;
import cz.o2.proxima.util.Classpath;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.Cancellable;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.o2.proxima.view.input.DataSourceUtils;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Strings;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.function.Consumer;

/**
 * Kafka writer and commit log using {@code KafkaProducer}.
 */
public class KafkaCommitLog extends AbstractOnlineAttributeWriter
    implements CommitLogReader, DataAccessor, PartitionedView {

  /**
   * Data read from a kafka partition.
   */
  private static final class KafkaStreamElement extends StreamElement {

    @Getter
    private final int partition;
    @Getter
    private final long offset;

    KafkaStreamElement(
        EntityDescriptor entityDesc,
        AttributeDescriptor attributeDesc,
        String uuid, String key, String attribute,
        long stamp, byte[] value, int partition, long offset) {

      super(entityDesc, attributeDesc, uuid, key, attribute, stamp, value);
      this.partition = partition;
      this.offset = offset;
    }

    @Override
    public String toString() {
      return "KafkaStreamElement(entityDesc=" + getEntityDescriptor()
          + ", attributeDesc=" + getAttributeDescriptor()
          + ", uuid=" + getUuid()
          + ", key=" + getKey()
          + ", attribute=" + getAttribute()
          + ", stamp=" + getStamp()
          + ", value.length=" + (getValue() == null ? 0 : getValue().length)
          + ", partition=" + partition
          + ", offset=" + offset
          + ")";
    }

  }

  /**
   * Consumer of stream elements.
   * The callback may or might not be called depending on the consuming mode
   * (bulk or online).
   */
  static interface ElementConsumer {
    void consumeWithConfirm(
        @Nullable StreamElement element,
        TopicPartition tp, long offset,
        Consumer<Throwable> errorHandler);
  }

  static interface TopicPartitionCommitter {
    void commit(TopicPartition tp, long offset);
  }

  static final class OnlineConsumer implements ElementConsumer {
    final LogObserver observer;
    final TopicPartitionCommitter committer;
    OnlineConsumer(LogObserver observer, TopicPartitionCommitter committer) {
      this.observer = observer;
      this.committer = committer;
    }

    @Override
    public void consumeWithConfirm(
        @Nullable StreamElement element,
        TopicPartition tp, long offset,
        Consumer<Throwable> errorHandler) {

      if (element != null) {
        observer.onNext(element, tp::partition, (succ, exc) -> {
          if (succ) {
            committer.commit(tp, offset);
          } else {
            errorHandler.accept(exc);
          }
        });
      } else {
        committer.commit(tp, offset);
      }
    }
  }

  static final class BulkConsumer implements ElementConsumer {
    final BulkLogObserver observer;
    final TopicPartitionCommitter committer;
    BulkConsumer(BulkLogObserver observer, TopicPartitionCommitter committer) {
      this.observer = observer;
      this.committer = committer;
    }

    @Override
    public void consumeWithConfirm(
        @Nullable StreamElement element,
        TopicPartition tp, long offset,
        Consumer<Throwable> errorHandler) {

      if (element != null) {
        observer.onNext(element, tp::partition, (succ, exc) -> {
          if (succ) {
            committer.commit(tp, offset);
          } else {
            errorHandler.accept(exc);
          }
        });
      }
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(KafkaCommitLog.class);

  /** A poll interval in milliseconds. */
  public static final String POLL_INTERVAL_CFG = "poll.interval";
  /** Partitioner class for entity key-attribute pair. */
  public static final String PARTITIONER_CLASS = "partitioner";

  public static final String WRITER_CONFIG_PREFIX = "kafka.";
  private static final int PRODUCE_CONFIG_PREFIX_LENGTH = WRITER_CONFIG_PREFIX.length();

  @Getter
  private final String topic;

  private final Map<String, Object> cfg;
  private final AtomicBoolean shutdown = new AtomicBoolean();

  private KafkaProducer<String, byte[]> producer;
  private long consumerPollInterval = 100;

  Partitioner partitioner = new KeyPartitioner();

  public KafkaCommitLog(
      EntityDescriptor entity,
      URI uri,
      Map<String, Object> cfg) {

    super(entity, uri);

    if (uri.getPath().length() <= 1) {
      throw new IllegalArgumentException("Specify topic by path in URI");
    }
    if (Strings.isNullOrEmpty(uri.getAuthority())) {
      throw new IllegalArgumentException("Specify brokers by authority in URI");
    }

    this.cfg = cfg;
    this.topic = Utils.topic(uri);
    configure(cfg);
  }


  private void configure(Map<String, Object> cfg) {
    this.consumerPollInterval = Optional.ofNullable(cfg.get(POLL_INTERVAL_CFG))
        .map(v -> Long.valueOf(v.toString()))
        .orElse(consumerPollInterval);

    this.partitioner = Optional.ofNullable((String) cfg.get(PARTITIONER_CLASS))
        .map(cls -> {
          try {
            return Classpath.findClass(cls, Partitioner.class);
          } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
          }
        })
        .map(cls -> {
          try {
            return cls.newInstance();
          } catch (InstantiationException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
          }
        })
        .orElse(this.partitioner);

    LOG.info(
        "Using consumerPollInterval {} and partitionerClass {} for URI {}",
        consumerPollInterval, partitioner.getClass(), getURI());
  }


  @Override
  public List<Partition> getPartitions() {
    final List<PartitionInfo> partitions;
    if (this.producer == null) {
      try (KafkaConsumer<String, byte[]> consumer = createConsumer()) {
        partitions = consumer.partitionsFor(topic);
      }
    } else {
      partitions = this.producer.partitionsFor(topic);
    }
    return partitions.stream().map(pi ->
      new Partition() {
        @Override
        public int getId() {
          return pi.partition();
        }
      }
    ).collect(Collectors.toList());
  }


  @Override
  @SuppressWarnings("unchecked")
  public void write(StreamElement data, CommitCallback callback) {
    try {
      if (this.producer == null) {
        this.producer = createProducer();
      }
      int partition = (partitioner.getPartitionId(
          data.getKey(), data.getAttribute(), data.getValue()) & Integer.MAX_VALUE)
          % producer.partitionsFor(topic).size();
      producer.send(
          new ProducerRecord(topic, partition, data.getStamp(), data.getKey()
              + "#" + data.getAttribute(), data.getValue()),
              (metadata, exception) -> callback.commit(exception == null, exception));
    } catch (Exception ex) {
      LOG.warn("Failed to write ingest {}", data, ex);
      callback.commit(false, ex);
    }
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

    // we need to remap the input here to be able to directly persist it again
    return MapElements.of(
        flow.createInput(DataSourceUtils.fromPartitions(
            DataSourceUtils.fromBlockingQueue(queue, producer))))
        .using(e -> e)
        .output();
  }

  @Override
  public <T> Dataset<T> observe(
      Flow flow, String name, PartitionedLogObserver<T> observer) {

    BlockingQueue<T> queue = new SynchronousQueue<>();

    DataSourceUtils.Producer producer = () -> {
      observePartitions(name, null, Position.NEWEST, false,
          Utils.forwardingTo(queue, observer),
          Utils.rebalanceListener(observer));
    };

    // we need to remap the input here to be able to directly persist it again
    return MapElements.of(
        flow.createInput(DataSourceUtils.fromPartitions(
            DataSourceUtils.fromBlockingQueue(queue, producer))))
        .using(e -> e)
        .output();
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

    // start new thread that will fill our observer
    Thread consumer = new Thread(() -> {
      try {
        try (KafkaConsumer<String, byte[]> kafkaConsumer = createConsumer(
            name, partitions, listener, position)) {
          if (partitions != null) {
            List<TopicPartition> assignment = partitions.stream()
                .map(p -> new TopicPartition(topic, p.getId()))
                .collect(Collectors.toList());
            kafkaConsumer.assign(assignment);
          }
          latch.countDown();
          processConsumer(
              kafkaConsumer,
              name != null,
              name != null ? false : stopAtCurrent,
              observer);
        }
      } catch (Throwable thwbl) {
        LOG.error("Error in running the observer {}", name, thwbl);
        observer.onError(thwbl);
      }
    });
    consumer.setDaemon(true);
    consumer.setName("consumer-" + name);
    consumer.start();

    try {
      LOG.debug("Waiting for the consumer {} to be created and run", name);
      latch.await();
    } catch (InterruptedException ex) {
      LOG.warn("Interrupted while waiting for the creation of the consumer.", ex);
      Thread.currentThread().interrupt();
    }

    return () -> {
      consumer.interrupt();
    };
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
                LOG.info(
                    "Seeking to offset {} for consumer name {} on partition {}",
                    off.offset(), name, tp);
                consumerRef.get().seek(tp, off.offset());
              } else {
                LOG.debug(
                    "Partition {} for consumer name {} has no committed offset",
                    tp, name);
              }
            }
          }
        } catch (Exception | Error err) {
          LOG.error(
              "Failed to seek to committed offsets for {}",
              partitions, err);
          throw new RuntimeException(err);
        }
      }

    };

    // wait until the consumer is really created
    CountDownLatch latch = new CountDownLatch(1);

    // start new thread that will fill our observer
    Thread consumer = new Thread(() -> {

      consumerRef.set(createConsumer(name, null, listener, position));
      try {
        latch.countDown();
        processConsumer(consumerRef.get(), true, false, observer);
      } catch (Exception exc) {
        LOG.error("Exception in running the observer {}", name, exc);
        observer.onError(exc);
      } catch (Error err) {
        LOG.error("Error in running the observer {}", name, err);
        observer.onError(err);
      } finally {
        consumerRef.get().close();
      }
    });
    consumer.setDaemon(true);
    consumer.setName("consumer-" + name);
    consumer.start();

    try {
      LOG.debug("Waiting for the consumer {} to be created and run", name);
      latch.await();
    } catch (InterruptedException ex) {
      LOG.warn("Interrupted while waiting for the creation of the consumer.", ex);
      Thread.currentThread().interrupt();
    }
    return () -> {
      consumer.interrupt();
    };
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

    OnlineConsumer onlineConsumer = new OnlineConsumer(observer, (tp, offset) -> {
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

    BulkConsumer bulkConsumer = new BulkConsumer(observer, (tp, offset) -> {
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
      ElementConsumer consumer,
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
        // in kafka, each entity attribute is separeted by `#' from entity key
        int hashPos = key.lastIndexOf("#");
        KafkaStreamElement ingest = null;
        if (hashPos < 0 || hashPos >= key.length()) {
          LOG.error("Invalid key in kafka topic: {}", key);
        } else {
          String entityKey = key.substring(0, hashPos);
          String attribute = key.substring(hashPos + 1);
          Optional<AttributeDescriptor<?>> attr = getEntityDescriptor().findAttribute(attribute);
          if (!attr.isPresent()) {
            LOG.error("Invalid attribute in kafka key {}", key);
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
        LOG.info("Reached end of current data. Terminating consumption.");
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


  @SuppressWarnings("unchecked")
  private KafkaProducer<String, byte[]> createProducer() {
    Properties props = createProps();
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getURI().getAuthority());
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    return new KafkaProducer<>(
        props, Serdes.String().serializer(), Serdes.ByteArray().serializer());
  }


  @SuppressWarnings("unchecked")
  private Properties createProps() {
    Properties props = new Properties();
    for (Map.Entry<String, Object> e : cfg.entrySet()) {
      if (e.getKey().startsWith(WRITER_CONFIG_PREFIX)) {
        props.put(e.getKey().substring(PRODUCE_CONFIG_PREFIX_LENGTH),
            e.getValue().toString());
      }
    }
    return props;
  }

  /** Create kafka consumer with specific rebalance listener. */
  public KafkaConsumerFactory createConsumerFactory() {
    return new KafkaConsumerFactory(getURI(), createProps());
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

    KafkaConsumerFactory factory = createConsumerFactory();
    final KafkaConsumer<String, byte[]> consumer;

    if (name != null) {
      if (listener != null) {
        consumer = factory.create(name, listener);
      } else {
        consumer = factory.create(name);
      }
    } else if (partitions != null) {
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
    if (this.producer != null) {
      this.producer.close();
      this.producer = null;
    }
    this.shutdown.set(true);
  }

  @Override
  public Optional<AttributeWriterBase> getWriter() {
    return Optional.of(this);
  }

  @Override
  public Optional<CommitLogReader> getCommitLogReader() {
    return Optional.of(this);
  }

  @Override
  public Optional<PartitionedView> getPartitionedView() {
    return Optional.of(this);
  }



}
