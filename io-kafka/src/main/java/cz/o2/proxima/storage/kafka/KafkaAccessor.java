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

import cz.o2.proxima.view.LocalCachedPartitionedView;
import cz.o2.proxima.storage.commitlog.Partitioner;
import com.google.common.base.Strings;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.KeyPartitioner;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.view.PartitionedCachedView;
import cz.o2.proxima.view.PartitionedView;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.AccessLevel;

/**
 * Kafka writer and commit log using {@code KafkaProducer}.
 */
@Slf4j
public class KafkaAccessor extends AbstractStorage implements DataAccessor {

  /** A poll interval in milliseconds. */
  public static final String POLL_INTERVAL_CFG = "poll.interval";
  /** Partitioner class for entity key-attribute pair. */
  public static final String PARTITIONER_CLASS = "partitioner";

  public static final String WRITER_CONFIG_PREFIX = "kafka.";
  private static final int PRODUCE_CONFIG_PREFIX_LENGTH = WRITER_CONFIG_PREFIX.length();

  @Getter
  private final String topic;

  private final Map<String, Object> cfg;

  @Getter(AccessLevel.PACKAGE)
  private Partitioner partitioner = new KeyPartitioner();

  @Getter(AccessLevel.PACKAGE)
  private long consumerPollInterval = 100;

  public KafkaAccessor(
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
        .map(cls -> Classpath.findClass(cls, Partitioner.class))
        .map(cls -> {
          try {
            return cls.newInstance();
          } catch (InstantiationException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
          }
        })
        .orElse(this.partitioner);

    log.info(
        "Using consumerPollInterval {} and partitionerClass {} for URI {}",
        consumerPollInterval, partitioner.getClass(), getURI());
  }


  @SuppressWarnings("unchecked")
  Properties createProps() {
    Properties props = new Properties();
    for (Map.Entry<String, Object> e : cfg.entrySet()) {
      if (e.getKey().startsWith(WRITER_CONFIG_PREFIX)) {
        props.put(e.getKey().substring(PRODUCE_CONFIG_PREFIX_LENGTH),
            e.getValue().toString());
      }
    }
    return props;
  }

  /**
   * Create kafka consumer with specific rebalance listener.
   * @return {@link KafkaConsumerFactory} for creating consumers
   */
  public KafkaConsumerFactory createConsumerFactory() {
    return new KafkaConsumerFactory(getURI(), createProps());
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(new KafkaWriter(this));
  }

  @Override
  public Optional<CommitLogReader> getCommitLogReader(Context context) {
    return Optional.of(new KafkaLogReader(this, context));
  }

  @Override
  public Optional<PartitionedView> getPartitionedView(Context context) {
    return Optional.of(new KafkaLogReader(this, context));
  }

  @Override
  public Optional<PartitionedCachedView> getCachedView(Context context) {
    return Optional.of(new LocalCachedPartitionedView(
        getEntityDescriptor(),
        new KafkaLogReader(this, context),
        getWriter(context).get().online(),
        context::getExecutorService));
  }

}
