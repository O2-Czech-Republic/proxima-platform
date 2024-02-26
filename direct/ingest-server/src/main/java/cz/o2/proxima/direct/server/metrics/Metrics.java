/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.server.metrics;

import cz.o2.proxima.core.metrics.ApproxPercentileMetric;
import cz.o2.proxima.core.metrics.GaugeMetric;
import cz.o2.proxima.core.metrics.Metric;
import cz.o2.proxima.core.metrics.MetricFactory;
import cz.o2.proxima.core.metrics.TimeAveragingMetric;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.util.Pair;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Metrics related to the ingest server. */
@Slf4j
public class Metrics {
  public static final String GROUP = "cz.o2.proxima.direct.server";
  private static final MetricFactory FACTORY = new MetricFactory();
  private static final Map<String, Metric<?>> CACHED_METRICS = new ConcurrentHashMap<>();

  public static final TimeAveragingMetric INGEST_SINGLE =
      FACTORY.timeAveraging(GROUP, "ingest-single", 1_000);

  public static final TimeAveragingMetric INGEST_BULK =
      FACTORY.timeAveraging(GROUP, "ingest-bulk", 1_000);

  public static final ApproxPercentileMetric BULK_SIZE =
      FACTORY.percentile(
          GROUP, "bulk-size", Duration.ofHours(1).toMillis(), Duration.ofMinutes(5).toMillis());

  public static final TimeAveragingMetric INGESTS = FACTORY.timeAveraging(GROUP, "ingests", 1_000);

  public static final TimeAveragingMetric COMMIT_LOG_APPEND =
      FACTORY.timeAveraging(GROUP, "commit-log-append", 1_000);

  public static final TimeAveragingMetric GET_REQUESTS =
      FACTORY.timeAveraging(GROUP, "get-requests", 1_000);

  public static final TimeAveragingMetric LIST_REQUESTS =
      FACTORY.timeAveraging(GROUP, "list-requests", 1_000);

  public static final TimeAveragingMetric UPDATE_REQUESTS =
      FACTORY.timeAveraging(GROUP, "update-requests", 1_000);

  public static final TimeAveragingMetric DELETE_REQUESTS =
      FACTORY.timeAveraging(GROUP, "delete-requests", 1_000);

  public static final TimeAveragingMetric DELETE_WILDCARD_REQUESTS =
      FACTORY.timeAveraging(GROUP, "delete-wildcard-requests", 1_000);

  public static final TimeAveragingMetric NON_COMMIT_LOG_UPDATES =
      FACTORY.timeAveraging(GROUP, "non-commit-updates", 1_000);

  public static final TimeAveragingMetric NON_COMMIT_LOG_DELETES =
      FACTORY.timeAveraging(GROUP, "non-commit-deletes", 1_000);

  public static final TimeAveragingMetric COMMIT_UPDATE_DISCARDED =
      FACTORY.timeAveraging(GROUP, "commits-discarded", 1_000);

  public static final TimeAveragingMetric NON_COMMIT_WRITES_RETRIES =
      FACTORY.timeAveraging(GROUP, "non-commit-retries", 1_000);

  public static final TimeAveragingMetric INVALID_REQUEST =
      FACTORY.timeAveraging(GROUP, "invalid-request", 1_000);

  public static final GaugeMetric LIVENESS = FACTORY.gauge(GROUP, "liveness");

  public static final ApproxPercentileMetric INGEST_LATENCY =
      FACTORY.percentile(GROUP, "ingest-latency", 60_000, 1_000);

  private static final Map<String, Pair<Boolean, GaugeMetric>> consumerMetrics =
      Collections.synchronizedMap(new HashMap<>());

  @SuppressWarnings("unchecked")
  public static Metric<Double> ingestsForAttribute(AttributeDescriptor<?> attr) {
    return (Metric)
        CACHED_METRICS.computeIfAbsent(
            attr.getEntity() + "_" + getAttrNameForJMX(attr) + "_ingests",
            name -> FACTORY.timeAveraging(GROUP, name, 1_000));
  }

  static String getAttrNameForJMX(AttributeDescriptor<?> attr) {
    return attr.isWildcard() ? attr.toAttributePrefix(false) : attr.getName();
  }

  public static ApproxPercentileMetric sizeForAttribute(AttributeDescriptor<?> attr) {
    return (ApproxPercentileMetric)
        CACHED_METRICS.computeIfAbsent(
            attr.getEntity() + "_" + getAttrNameForJMX(attr) + "_size",
            name ->
                FACTORY.percentile(
                    GROUP, name, Duration.ofHours(1).toMillis(), Duration.ofMinutes(5).toMillis()));
  }

  public static void reportConsumerWatermark(
      String consumer, boolean bulk, long watermark, long elementTimestamp) {

    consumerWatermark(consumer, bulk, watermark);
    consumerWatermarkLag(consumer, watermark);
    // Element timestamp is set to -1 for onIdle methods.
    if (elementTimestamp >= 0) {
      consumerWatermarkDiff(consumer, watermark, elementTimestamp);
    }
  }

  private static void consumerWatermark(String consumer, boolean bulk, long watermark) {
    GaugeMetric metric =
        (GaugeMetric)
            CACHED_METRICS.computeIfAbsent(
                toJmxCompatibleConsumerName(consumer) + "_watermark",
                name -> FACTORY.gauge(GROUP, name));
    consumerMetrics.putIfAbsent(consumer, Pair.of(bulk, metric));
    metric.increment((double) watermark);
  }

  private static void consumerWatermarkLag(String consumer, long watermark) {
    long lag = System.currentTimeMillis() - watermark;
    CACHED_METRICS
        .computeIfAbsent(
            toJmxCompatibleConsumerName(consumer) + "_watermark_lag",
            name ->
                FACTORY.percentile(
                    GROUP, name, Duration.ofHours(1).toMillis(), Duration.ofMinutes(5).toMillis()))
        .increment((double) lag);
  }

  private static void consumerWatermarkDiff(
      String consumer, long watermark, long elementTimestamp) {
    final long diff = elementTimestamp - watermark;
    CACHED_METRICS
        .computeIfAbsent(
            toJmxCompatibleConsumerName(consumer) + "_watermark_diff",
            name ->
                FACTORY.percentile(
                    GROUP, name, Duration.ofHours(1).toMillis(), Duration.ofMinutes(5).toMillis()))
        .increment((double) diff);
  }

  private static String toJmxCompatibleConsumerName(String consumer) {
    return consumer.replace('-', '_');
  }

  public static Pair<Long, Long> minWatermarkOfConsumers() {
    synchronized (consumerMetrics) {
      long minBulkWatermark = Watermarks.MAX_WATERMARK;
      long minOnlineWatermark = Watermarks.MAX_WATERMARK;
      for (Pair<Boolean, GaugeMetric> p : consumerMetrics.values()) {
        if (p.getFirst()) {
          if (minBulkWatermark > p.getSecond().getValue().longValue()) {
            minBulkWatermark = p.getSecond().getValue().longValue();
          }
        } else if (minOnlineWatermark > p.getSecond().getValue().longValue()) {
          minOnlineWatermark = p.getSecond().getValue().longValue();
        }
      }
      return Pair.of(minOnlineWatermark, minBulkWatermark);
    }
  }

  public static List<String> consumerWatermarkLags() {
    long now = System.currentTimeMillis();
    synchronized (consumerMetrics) {
      return consumerMetrics.entrySet().stream()
          .map(
              e ->
                  String.format(
                      "%s(%b:%d)",
                      e.getKey(),
                      e.getValue().getFirst(),
                      now - e.getValue().getSecond().getValue().longValue()))
          .collect(Collectors.toList());
    }
  }

  private Metrics() {
    // nop
  }
}
