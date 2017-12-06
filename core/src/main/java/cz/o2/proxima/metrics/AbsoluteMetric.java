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

package cz.o2.proxima.metrics;

import cz.seznam.euphoria.shaded.guava.com.google.common.util.concurrent.AtomicDouble;
import java.beans.ConstructorProperties;

/**
 * A metric with absolute value.
 */
public class AbsoluteMetric extends ScalarMetric {

  public static AbsoluteMetric of(String group, String name) {
    return new AbsoluteMetric(group, name);
  }

  AtomicDouble value = new AtomicDouble();

  @ConstructorProperties({ "group", "name" })
  public AbsoluteMetric(String group, String name) {
    super(group, name);
  }

  @Override
  public void increment(double d) {
    value.addAndGet(d);
  }

  @Override
  public Double getValue() {
    return value.get();
  }

  public void setValue(double v) {
    value.set(v);
  }

  @Override
  public void reset() {
    setValue(0.0);
  }

}
