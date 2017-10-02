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

package cz.o2.proxima.server.metrics;

import cz.o2.proxima.metrics.ApproxPercentileMetric;
import cz.o2.proxima.metrics.Metric;
import cz.o2.proxima.metrics.TimeAveragingMetric;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Metrics related to the ingest server.
 */
public class Metrics {

  private static final String GROUP = "cz.o2.proxima.server";

  public static final Metric<Double> INGEST_SINGLE = TimeAveragingMetric.of(
      GROUP, "ingest-single", 1_000);

  public static final Metric<Double> INGEST_BULK = TimeAveragingMetric.of(
      GROUP, "ingest-bulk", 1_000);

  public static final ApproxPercentileMetric BULK_SIZE = ApproxPercentileMetric.of(
      GROUP, "bulk-size");

  public static final Metric<Double> INGESTS = TimeAveragingMetric.of(
      GROUP, "ingests", 1_000);

  public static final Metric<Double> COMMIT_LOG_APPEND = TimeAveragingMetric.of(
      GROUP, "commit-log-append", 1_000);

  public static final Metric<Double> GET_REQUESTS = TimeAveragingMetric.of(
      GROUP, "get-requests", 1_000);

  public static final Metric<Double> LIST_REQUESTS = TimeAveragingMetric.of(
      GROUP, "list-requests", 1_000);

  public static final Metric<Double> UPDATE_REQUESTS = TimeAveragingMetric.of(
      GROUP, "update-requests", 1_000);

  public static final Metric<Double> DELETE_REQUESTS = TimeAveragingMetric.of(
      GROUP, "delete-requests", 1_000);

  public static final Metric<Double> DELETE_WILDCARD_REQUESTS = TimeAveragingMetric.of(
      GROUP, "delete-wildcard-requests", 1_000);

  public static final Metric<Double> NON_COMMIT_LOG_UPDATES = TimeAveragingMetric.of(
      GROUP, "non-commit-updates", 1_000);

  public static final Metric<Double> NON_COMMIT_LOG_DELETES = TimeAveragingMetric.of(
      GROUP, "non-commit-deletes", 1_000);

  public static final Metric<Double> COMMIT_UPDATE_DISCARDED = TimeAveragingMetric.of(
      GROUP, "commits-discarded", 1_000);

  public static final Metric<Double> NON_COMMIT_WRITES_RETRIES = TimeAveragingMetric.of(
      GROUP, "non-commit-retries", 1_000);

  public static final Metric<Double> INVALID_REQUEST = TimeAveragingMetric.of(
      GROUP, "invalid-request", 1_000);

  public static final Metric<Double> INVALID_ENTITY = TimeAveragingMetric.of(
      GROUP, "invalid-entity", 1_000);

  public static final Metric<Double> INVALID_ATTRIBUTE = TimeAveragingMetric.of(
      GROUP, "invalid-attribute", 1_000);

  private static final Metric[] ALL = {
    INGEST_SINGLE,
    INGEST_BULK,
    BULK_SIZE,
    INGESTS,
    COMMIT_LOG_APPEND,
    GET_REQUESTS,
    LIST_REQUESTS,
    UPDATE_REQUESTS,
    DELETE_REQUESTS,
    DELETE_WILDCARD_REQUESTS,
    NON_COMMIT_LOG_UPDATES,
    NON_COMMIT_LOG_DELETES,
    COMMIT_UPDATE_DISCARDED,
    NON_COMMIT_WRITES_RETRIES,
    INVALID_REQUEST,
    INVALID_ENTITY,
    INVALID_ATTRIBUTE
  };


  public static void register() {

    MBeanServer mbs =  ManagementFactory.getPlatformMBeanServer();
    try {
      for (Metric m : ALL) {
        ObjectName mxbeanName = new ObjectName(
            m.getGroup() + "." + m.getName() + ":type=" + m.getClass().getSimpleName());
        mbs.registerMBean(m, mxbeanName);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
