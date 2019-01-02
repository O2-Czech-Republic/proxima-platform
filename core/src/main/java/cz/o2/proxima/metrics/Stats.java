/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
import java.util.Arrays;

/**
 * Stats aggregating the distribution percentiles.
 */
@Stable
public class Stats implements StatsMBean {

  private final double[] raw;

  Stats(double[] raw) {
    this.raw = raw;
  }

  @Override
  public double get1() {
    return raw[0];
  }

  @Override
  public double get10() {
    return raw[1];
  }

  @Override
  public double get30() {
    return raw[2];
  }

  @Override
  public double get50() {
    return raw[3];
  }

  @Override
  public double get70() {
    return raw[4];
  }

  @Override
  public double get90() {
    return raw[5];
  }

  @Override
  public double get99() {
    return raw[6];
  }

  @Override
  public String toString() {
    return Arrays.toString(raw);
  }

  double[] getRaw() {
    return raw;
  }

}