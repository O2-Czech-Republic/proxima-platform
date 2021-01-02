/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.metrics;

import cz.o2.proxima.annotations.Stable;
import java.beans.ConstructorProperties;
import java.io.Serializable;
import lombok.Getter;

/**
 * A single metric. A single metric might be a single number or a vector of numbers (e.g. say
 * percentile statistics).
 */
@Stable
public abstract class Metric<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  @Getter final String group;

  @Getter final String name;

  @ConstructorProperties({"group", "name"})
  public Metric(String group, String name) {
    this.group = group;
    this.name = name;
  }

  /** Increment the metric by one. */
  public void increment() {
    increment(1.0);
  }

  /**
   * Increment the metric by given double value.
   *
   * @param increment the value to increment the metric by
   */
  public abstract void increment(double increment);

  /** Decrement metric by one. */
  public void decrement() {
    increment(-1.0);
  }

  /**
   * Retrieve current value of the metric.
   *
   * @return current value
   */
  public abstract T getValue();

  /** Reset the metric to initial state. */
  public abstract void reset();
}
