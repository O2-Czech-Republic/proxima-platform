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
package cz.o2.proxima.core.metrics;

import cz.o2.proxima.core.functional.Factory;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class MetricFactory {

  private final Map<String, Metric<?>> metricCache = new ConcurrentHashMap<>();

  private final MetricsRegistrar registrar = findMetricsRegistrar();

  private MetricsRegistrar findMetricsRegistrar() {
    final ServiceLoader<MetricsRegistrar> loader;
    if (getClass().getModule().getLayer() != null) {
      loader = ServiceLoader.load(getClass().getModule().getLayer(), MetricsRegistrar.class);
    } else {
      loader = ServiceLoader.load(MetricsRegistrar.class);
    }
    return loader.stream()
        .filter(p -> !p.type().equals(JmxMetricsRegistrar.class))
        .map(ServiceLoader.Provider::get)
        .findAny()
        .orElseGet(JmxMetricsRegistrar::new);
  }

  public AbsoluteMetric absolute(String group, String name) {
    return getOrCreate(group, name, () -> new AbsoluteMetric(group, name));
  }

  public TimeAveragingMetric timeAveraging(
      String group, String name, long windowLength, long checkpointMs, long purgeMs) {

    return new TimeAveragingMetric(group, name, windowLength, checkpointMs, purgeMs);
  }

  public TimeAveragingMetric timeAveraging(String group, String name, long windowLengthMs) {
    // by default, checkpoint every window length and purge after thirty windows
    return timeAveraging(group, name, windowLengthMs, windowLengthMs, 30 * windowLengthMs);
  }

  /**
   * Construct the metric.
   *
   * @param group group name
   * @param name metric name
   * @param duration total duration of the statistic in ms
   * @param window windowNs size in ms
   * @return the metric
   */
  public ApproxPercentileMetric percentile(String group, String name, long duration, long window) {
    return getOrCreate(
        group, name, () -> new ApproxPercentileMetric(group, name, duration, window));
  }

  /**
   * Construct the metric.
   *
   * @param group group of the metric
   * @param name name of the metric
   * @return new gauge metric
   */
  public GaugeMetric gauge(String group, String name) {
    return getOrCreate(group, name, () -> new GaugeMetric(group, name));
  }

  void register(Metric<?> metric) {
    registrar.register(metric);
  }

  @SuppressWarnings("unchecked")
  private <T, M extends Metric<T>> M getOrCreate(String group, String name, Factory<M> factory) {

    String metricName = group + "." + name;
    return (M)
        metricCache.computeIfAbsent(
            metricName,
            tmp -> {
              M metric = factory.apply();
              register(metric);
              return metric;
            });
  }
}
