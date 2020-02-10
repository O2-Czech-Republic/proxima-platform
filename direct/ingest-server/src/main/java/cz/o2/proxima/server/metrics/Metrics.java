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
package cz.o2.proxima.server.metrics;

import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.metrics.ApproxPercentileMetric;
import cz.o2.proxima.metrics.Metric;
import cz.o2.proxima.metrics.TimeAveragingMetric;
import cz.o2.proxima.repository.AttributeDescriptor;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import lombok.extern.slf4j.Slf4j;

/** Metrics related to the ingest server. */
@Slf4j
public class Metrics {

  private static final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
  private static final Map<String, Metric<?>> METRICS = new ConcurrentHashMap<>();
  public static final String GROUP = "cz.o2.proxima.server";

  public static final Metric<Double> INGEST_SINGLE =
      getOrCreate("ingest-single", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> INGEST_BULK =
      getOrCreate("ingest-bulk", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final ApproxPercentileMetric BULK_SIZE =
      getOrCreate(
          "bulk-size",
          name ->
              ApproxPercentileMetric.of(
                  GROUP, name, Duration.ofHours(1).toMillis(), Duration.ofMinutes(5).toMillis()));

  public static final Metric<Double> INGESTS =
      getOrCreate("ingests", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> COMMIT_LOG_APPEND =
      getOrCreate("commit-log-append", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> GET_REQUESTS =
      getOrCreate("get-requests", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> LIST_REQUESTS =
      getOrCreate("list-requests", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> UPDATE_REQUESTS =
      getOrCreate("update-requests", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> DELETE_REQUESTS =
      getOrCreate("delete-requests", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> DELETE_WILDCARD_REQUESTS =
      getOrCreate("delete-wildcard-requests", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> NON_COMMIT_LOG_UPDATES =
      getOrCreate("non-commit-updates", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> NON_COMMIT_LOG_DELETES =
      getOrCreate("non-commit-deletes", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> COMMIT_UPDATE_DISCARDED =
      getOrCreate("commits-discarded", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> NON_COMMIT_WRITES_RETRIES =
      getOrCreate("non-commit-retries", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> INVALID_REQUEST =
      getOrCreate("invalid-request", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> INVALID_ENTITY =
      getOrCreate("invalid-entity", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static final Metric<Double> INVALID_ATTRIBUTE =
      getOrCreate("invalid-attribute", name -> TimeAveragingMetric.of(GROUP, name, 1_000));

  public static Metric<Double> ingestsForAttribute(AttributeDescriptor<?> attr) {
    return getOrCreate(
        String.format("%s_%s_ingests", attr.getEntity(), getAttrNameForJMX(attr)),
        name -> TimeAveragingMetric.of(GROUP, name, 1_000));
  }

  static String getAttrNameForJMX(AttributeDescriptor<?> attr) {
    return attr.isWildcard() ? attr.toAttributePrefix(false) : attr.getName();
  }

  public static ApproxPercentileMetric sizeForAttribute(AttributeDescriptor<?> attr) {
    return getOrCreate(
        String.format("%s_%s_size", attr.getEntity(), getAttrNameForJMX(attr)),
        name ->
            ApproxPercentileMetric.of(
                GROUP, name, Duration.ofHours(1).toMillis(), Duration.ofMinutes(5).toMillis()));
  }

  public static Metric<Double> consumerWatermark(String consumer) {
    return getOrCreate(
        String.format("%s_watermark", consumer.replaceAll("-", "_")),
        name -> TimeAveragingMetric.of(GROUP, name, 1_000));
  }

  public static ApproxPercentileMetric consumerWatermarkLag(String consumer) {
    return getOrCreate(
        String.format("%s_watermark_lag", consumer.replaceAll("-", "_")),
        name ->
            ApproxPercentileMetric.of(
                GROUP, name, Duration.ofHours(1).toMillis(), Duration.ofMinutes(5).toMillis()));
  }

  @SuppressWarnings("unchecked")
  public static <T, M extends Metric<T>> M getOrCreate(
      String name, UnaryFunction<String, M> factory) {
    return (M)
        METRICS.computeIfAbsent(
            name,
            tmp -> {
              M metric = factory.apply(name);
              registerWithMBeanServer(metric, mbs);
              return metric;
            });
  }

  /**
   * Register this metric with {@link MBeanServer}.
   *
   * @param mbs the MBeanServer
   */
  private static void registerWithMBeanServer(Metric m, MBeanServer mbs) {
    try {
      ObjectName mxbeanName =
          new ObjectName(
              m.getGroup() + "." + m.getName() + ":type=" + m.getClass().getSimpleName());
      mbs.registerMBean(m, mxbeanName);
    } catch (InstanceAlreadyExistsException
        | MBeanRegistrationException
        | NotCompliantMBeanException
        | MalformedObjectNameException ex) {
      log.warn("Failed to register metric {} with MBeanServer", m, ex);
    }
  }

  private Metrics() {
    // nop
  }
}
