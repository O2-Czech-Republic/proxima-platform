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
package cz.o2.proxima.direct.transaction.manager;

import cz.o2.proxima.core.metrics.GaugeMetric;
import cz.o2.proxima.core.metrics.MetricFactory;
import cz.o2.proxima.core.metrics.TimeAveragingMetric;
import lombok.Getter;

public class Metrics {

  private static final String GROUP = Metrics.class.getPackageName();

  private final MetricFactory factory = new MetricFactory();

  @Getter
  private final TimeAveragingMetric transactionsOpen =
      factory.timeAveraging(GROUP, "transactions_open", 1_000);

  @Getter
  private final TimeAveragingMetric transactionsUpdated =
      factory.timeAveraging(GROUP, "transactions_updated", 1_000);

  @Getter
  private final TimeAveragingMetric transactionsCommitted =
      factory.timeAveraging(GROUP, "transactions_committed", 1_000);

  @Getter
  private final TimeAveragingMetric transactionsRejected =
      factory.timeAveraging(GROUP, "transactions_rejected", 1_000);

  @Getter
  private final TimeAveragingMetric transactionsRolledBack =
      factory.timeAveraging(GROUP, "transactions_rolled_back", 1_000);

  @Getter private final GaugeMetric numWritesCached = factory.gauge(GROUP, "writes_cached");

  @Getter
  private final TimeAveragingMetric writesCleaned =
      factory.timeAveraging(GROUP, "writes_cleaned", 120_000, 60_000, 600_000);
}
