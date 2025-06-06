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
package cz.o2.proxima.direct.io.kafka;

import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.ConfigConstants;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage.SerializableAbstractStorage;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.KeyPartitioner;
import cz.o2.proxima.core.storage.commitlog.Partitioner;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.view.CachedView;
import cz.o2.proxima.direct.core.view.LocalCachedPartitionedView;
import cz.o2.proxima.direct.io.kafka.KafkaStreamElement.KafkaStreamElementSerializer;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.base.Strings;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

/** Kafka writer and commit log using {@code KafkaProducer}. */
@Slf4j
public class KafkaAccessor extends SerializableAbstractStorage implements DataAccessor {

  private static final long serialVersionUID = 1L;

  public static final String KAFKA_CONFIG_PREFIX = "kafka.";

  /** A poll interval in milliseconds. */
  public static final String POLL_INTERVAL_CFG = "poll.interval";

  /** Partitioner class for entity key-attribute pair. */
  public static final String PARTITIONER_CLASS = ConfigConstants.PARTITIONER;

  /**
   * Class performing parsing from (String, byte[]) to StreamElement. This kas to implement {@link
   * ElementSerializer} interface.
   */
  public static final String SERIALIZER_CLASS = "serializer-class";

  /** Maximal read speed in bytes per second. */
  public static final String MAX_BYTES_PER_SEC = "bytes-per-sec-max";

  /** Auto commit interval in milliseconds. */
  public static final String AUTO_COMMIT_INTERVAL_MS = "commit.auto-interval-ms";

  /** Log stale commit interval in milliseconds. */
  public static final String LOG_STALE_COMMIT_INTERVAL_MS = "commit.log-stale-interval-ms";

  /**
   * Timeout in milliseconds, that consumer should wait for group assignment before throwing an
   * exception.
   */
  public static final String ASSIGNMENT_TIMEOUT_MS = "assignment-timeout-ms";

  /**
   * Minimal time poll() has to return empty records, before first moving watermark to processing
   * time. This controls time needed to initialize kafka consumer.
   */
  public static final String EMPTY_POLL_TIME = "poll.allowed-empty-before-watermark-move";

  /** A name for a header containing sequential ID of {@link StreamElement} (if any). */
  public static final String SEQUENCE_ID_HEADER = "seqId";

  /** A name for a header containing UUID of {@link StreamElement} */
  public static final String UUID_HEADER = "uuid";

  @Getter @Nullable private final String topic;

  @Getter @Nullable private final String topicPattern;

  @Getter private final Map<String, Object> cfg;

  @Getter private Partitioner partitioner = new KeyPartitioner();

  @Getter private long consumerPollInterval = 100;

  @Getter private long maxBytesPerSec = Long.MAX_VALUE;

  @Getter private long autoCommitIntervalMs = Long.MAX_VALUE;

  @Getter private long logStaleCommitIntervalMs = 60_000L;

  @Getter private long assignmentTimeoutMillis = 10_000L;

  @Getter private KafkaWatermarkConfiguration watermarkConfiguration;

  @Getter Class<ElementSerializer<?, ?>> serializerClass;

  public KafkaAccessor(EntityDescriptor entity, URI uri, Map<String, Object> cfg) {

    super(entity, uri);

    if (Strings.isNullOrEmpty(uri.getAuthority())) {
      throw new IllegalArgumentException("Specify brokers by authority in URI");
    }

    this.cfg = cfg;
    this.topic = Utils.topic(uri);
    this.topicPattern = Utils.topicPattern(uri);

    Preconditions.checkArgument(
        topic == null ^ topicPattern == null,
        "Please specify EITHER topic directly as path OR specify topic regex pattern via %s in URI %s",
        Utils.TOPIC_PATTERN_QUERY,
        uri);

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

    this.autoCommitIntervalMs =
        Optional.ofNullable(cfg.get(AUTO_COMMIT_INTERVAL_MS))
            .map(v -> Long.parseLong(v.toString()))
            .orElse(autoCommitIntervalMs);

    this.logStaleCommitIntervalMs =
        Optional.ofNullable(cfg.get(LOG_STALE_COMMIT_INTERVAL_MS))
            .map(v -> Long.parseLong(v.toString()))
            .orElse(logStaleCommitIntervalMs);

    this.assignmentTimeoutMillis =
        Optional.ofNullable(cfg.get(ASSIGNMENT_TIMEOUT_MS))
            .map(v -> Long.parseLong(v.toString()))
            .orElse(assignmentTimeoutMillis);

    @SuppressWarnings({"unchecked", "rawtypes"})
    Class<ElementSerializer<?, ?>> serializer =
        Optional.ofNullable(cfg.get(SERIALIZER_CLASS))
            .map(Object::toString)
            .map(c -> (Class) Classpath.findClass(c, ElementSerializer.class))
            .orElse(KafkaStreamElementSerializer.class);
    this.serializerClass = serializer;

    this.watermarkConfiguration = new KafkaWatermarkConfiguration(cfg);

    log.info(
        "Configured accessor with "
            + "consumerPollInterval {},"
            + "partitionerClass {}, "
            + "maxBytesPerSec {}, "
            + "watermarkConfiguration {}, "
            + "autoCommitIntervalNs {}, "
            + "logStaleCommitIntervalMs {}, "
            + "serializerClass {},"
            + "for URI {}",
        consumerPollInterval,
        partitioner.getClass(),
        maxBytesPerSec,
        watermarkConfiguration,
        autoCommitIntervalMs,
        logStaleCommitIntervalMs,
        serializerClass,
        getUri());
  }

  Properties createProps() {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.getUri().getAuthority());
    for (Map.Entry<String, Object> e : cfg.entrySet()) {
      if (e.getKey().startsWith(KAFKA_CONFIG_PREFIX)) {
        props.put(e.getKey().substring(KAFKA_CONFIG_PREFIX.length()), e.getValue().toString());
      }
    }
    return props;
  }

  @VisibleForTesting
  AdminClient createAdmin() {
    return AdminClient.create(createProps());
  }

  /**
   * Create kafka consumer with specific rebalance listener.
   *
   * @param <K> key type
   * @param <V> value type
   * @return {@link KafkaConsumerFactory} for creating consumers
   */
  public <K, V> KafkaConsumerFactory<K, V> createConsumerFactory() {
    ElementSerializer<K, V> serializer = getSerializer();
    return new KafkaConsumerFactory<>(
        getUri(), createProps(), serializer.keySerde(), serializer.valueSerde());
  }

  /**
   * Checker for kafka topics configuration
   *
   * @param familyDescriptor Attribute family descriptor.
   * @return true if check succeeded, otherwise false
   */
  @Override
  public boolean isAcceptable(AttributeFamilyDescriptor familyDescriptor) {
    // Force checks for data compacting on state-commit-log topics
    if (familyDescriptor.getAccess().isStateCommitLog()) {
      Preconditions.checkState(
          this.topic != null, "State commit log is not supported on topics specified by regexp.");
      try (AdminClient adminClient = createAdmin()) {
        final DescribeConfigsResult configsResult =
            adminClient.describeConfigs(
                Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, this.topic)));
        final Config config =
            ExceptionUtils.uncheckedFactory(
                () -> Iterables.getOnlyElement(configsResult.all().get().values()));
        final ConfigEntry cleanupPolicy = config.get(TopicConfig.CLEANUP_POLICY_CONFIG);
        return verifyCleanupPolicy(cleanupPolicy);
      }
    }
    return true;
  }

  @VisibleForTesting
  public boolean verifyCleanupPolicy(ConfigEntry cleanupPolicy) {
    if (cleanupPolicy != null
        && cleanupPolicy.value().contains(TopicConfig.CLEANUP_POLICY_COMPACT)) {
      return true;
    }
    log.warn(
        "Missing option [cleanup.policy=compact] of kafka topic [{}] with access type [state-commit-log].",
        topic);
    return false;
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

  KafkaWriter<?, ?> newWriter() {
    return new KafkaWriter<>(this);
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

  boolean isTopicRegex() {
    return topicPattern != null;
  }
}
