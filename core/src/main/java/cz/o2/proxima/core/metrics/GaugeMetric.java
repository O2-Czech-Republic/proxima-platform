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
package cz.o2.proxima.core.metrics;

/** A metric that holds value that is set to it. */
public class GaugeMetric extends ScalarMetric {

  private static final long serialVersionUID = 1L;

  double value = 0.0;

  GaugeMetric(String group, String name) {
    super(group, name);
  }

  @Override
  public void increment(double increment) {
    value = increment;
  }

  @Override
  public Double getValue() {
    return value;
  }

  @Override
  public void reset() {
    value = 0.0;
  }
}
