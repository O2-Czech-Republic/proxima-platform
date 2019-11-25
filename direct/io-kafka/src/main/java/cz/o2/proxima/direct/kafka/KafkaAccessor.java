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

import com.google.common.base.Strings;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.kafka.KafkaStreamElement.KafkaStreamElementSerializer;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.direct.view.LocalCachedPartitionedView;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.commitlog.KeyPartitioner;
import cz.o2.proxima.storage.commitlog.Partitioner;
import cz.o2.proxima.util.Classpath;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Kafka writer and commit log using {@code KafkaProducer}. */
@Slf4j
public class KafkaAccessor extends AbstractStorage implements DataAccessor {

  /** A poll interval in milliseconds. */
  public static final String POLL_INTERVAL_CFG = "poll.interval";
  /** Partitioner class for entity key-attribute pair. */
  public static final String PARTITIONER_CLASS = "partitioner";
  /**
   * Class performing parsing from (String, byte[]) to StreamElement. This kas to implement {@link
   * ElementSerializer} interface.
   */
  public static final String SERIALIZER_CLASS = "serializer-class";
  /** Maximal read speed in bytes per second. */
  public static final String MAX_BYTES_PER_SEC = "bytes-per-sec-max";
  /** Allowed timestamp skew between consumer and producer. */
  public static final String TIMESTAMP_SKEW = "timestamp-skew";
  /** Number of empty polls to consider partition empty. */
  public static final String EMPTY_POLLS = "poll.count-for-empty";
  /** Number of records per poll() */
  public static final String MAX_POLL_RECORDS = "max.poll.records";

  /**
   * Minimal time poll() has to return empty records, before first moving watermark to processing
   * time. This controls time needed to initialize kafka consumer.
   */
  public static final String EMPTY_POLL_TIME = "poll.allowed-empty-before-watermark-move";

  public static final String WRITER_CONFIG_PREFIX = "kafka.";
  private static final int PRODUCE_CONFIG_PREFIX_LENGTH = WRITER_CONFIG_PREFIX.length();

  @Getter private final String topic;

  private final Map<String, Object> cfg;

  @Getter(AccessLevel.PACKAGE)
  private Partitioner partitioner = new KeyPartitioner();

  @Getter(AccessLevel.PACKAGE)
  private long consumerPollInterval = 100;

  @Getter(AccessLevel.PACKAGE)
  private long maxBytesPerSec = Long.MAX_VALUE;

  @Getter(AccessLevel.PACKAGE)
  private long timestampSkew = 100;

  @Getter(AccessLevel.PACKAGE)
  private int emptyPolls = (int) (1000 / consumerPollInterval);

  @Getter(AccessLevel.PACKAGE)
  private int maxPollRecords = 500;

  Class<ElementSerializer<?, ?>> serializerClass;

  public KafkaAccessor(EntityDescriptor entity, URI uri, Map<String, Object> cfg) {

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
    this.consumerPollInterval =
        Optional.ofNullable(cfg.get(POLL_INTERVAL_CFG))
            .map(v -> Long.valueOf(v.toString()))
            .orElse(consumerPollInterval);

    this.partitioner =
        Optional.ofNullable((String) cfg.get(PARTITIONER_CLASS))
            .map(cls -> Classpath.newInstance(cls, Partitioner.class))
            .orElse(this.partitioner);

    this.maxBytesPerSec =
        Optional.ofNullable(cfg.get(MAX_BYTES_PER_SEC))
            .map(v -> Long.valueOf(v.toString()))
            .orElse(maxBytesPerSec);

    this.timestampSkew =
        Optional.ofNullable(cfg.get(TIMESTAMP_SKEW))
            .map(v -> Long.valueOf(v.toString()))
            .orElse(timestampSkew);

    this.emptyPolls =
        Optional.ofNullable(cfg.get(EMPTY_POLLS))
            .map(v -> Integer.valueOf(v.toString()))
            .orElse((int) (1000 / consumerPollInterval));

    this.maxPollRecords =
        Optional.ofNullable(cfg.get(MAX_POLL_RECORDS))
            .map(v -> Integer.valueOf(v.toString()))
            .orElse(maxPollRecords);

    @SuppressWarnings("unchecked")
    Class<ElementSerializer<?, ?>> serializer =
        Optional.ofNullable(cfg.get(SERIALIZER_CLASS))
            .map(Object::toString)
            .map(c -> (Class) Classpath.findClass(c, ElementSerializer.class))
            .orElse(KafkaStreamElementSerializer.class);
    this.serializerClass = serializer;

    log.info(
        "Configured accessor with "
            + "consumerPollInterval {},"
            + "partitionerClass {}, "
            + "maxBytesPerSec {}, "
            + "timestampSkew {}, "
            + "emptyPolls {}, "
            + "maxPollRecords {}, "
            + "serializerClass {},"
            + "for URI {}",
        consumerPollInterval,
        partitioner.getClass(),
        maxBytesPerSec,
        timestampSkew,
        emptyPolls,
        maxPollRecords,
        serializerClass,
        getUri());
  }

  @SuppressWarnings("unchecked")
  Properties createProps() {
    Properties props = new Properties();
    for (Map.Entry<String, Object> e : cfg.entrySet()) {
      if (e.getKey().startsWith(WRITER_CONFIG_PREFIX)) {
        props.put(e.getKey().substring(PRODUCE_CONFIG_PREFIX_LENGTH), e.getValue().toString());
      }
    }
    props.put(MAX_POLL_RECORDS, maxPollRecords);
    return props;
  }

  /**
   * Create kafka consumer with specific rebalance listener.
   *
   * @return {@link KafkaConsumerFactory} for creating consumers
   */
  public <K, V> KafkaConsumerFactory<K, V> createConsumerFactory() {
    ElementSerializer<K, V> serializer = getSerializer();
    return new KafkaConsumerFactory<>(
        getUri(), createProps(), serializer.keySerde(), serializer.valueSerde());
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(newWriter());
  }

  @Override
  public Optional<CommitLogReader> getCommitLogReader(Context context) {
    return Optional.of(newReader(context));
  }

  @Override
  public Optional<CachedView> getCachedView(Context context) {
    return Optional.of(
        new LocalCachedPartitionedView(getEntityDescriptor(), newReader(context), newWriter()));
  }

  KafkaWriter newWriter() {
    return new KafkaWriter(this);
  }

  KafkaLogReader newReader(Context context) {
    return new KafkaLogReader(this, context);
  }

  /**
   * Retrieve {@link ElementSerializer}.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @return the {@link ElementSerializer}.
   */
  @SuppressWarnings("unchecked")
  public <K, V> ElementSerializer<K, V> getSerializer() {
    ElementSerializer<K, V> res = (ElementSerializer<K, V>) Classpath.newInstance(serializerClass);
    res.setup(getEntityDescriptor());
    return res;
  }
}
