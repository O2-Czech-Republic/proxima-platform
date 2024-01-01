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

import cz.o2.proxima.core.annotations.Stable;

/** An MBean for distribution stats. */
@Stable
public interface StatsMBean {

  /**
   * @return 1st percentile.
   */
  double get1();

  /**
   * @return 10th percentile.
   */
  double get10();

  /**
   * @return 30th percentile.
   */
  double get30();

  /**
   * @return 50th percentile.
   */
  double get50();

  /**
   * @return 70th percentile.
   */
  double get70();

  /**
   * @return 90th percentile.
   */
  double get90();

  /**
   * @return 99th percentile.
   */
  double get99();
}
