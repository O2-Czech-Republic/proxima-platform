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
package cz.o2.proxima.time;

import cz.o2.proxima.annotations.Internal;

/**
 * Vector clock implementation.
 */
@Internal
public class VectorClock {

  /**
   * Create new instance of VectorClock.
   * @param dimensions dimensions of the clock
   * @return new instance
   */
  public static VectorClock of(int dimensions) {
    return new VectorClock(dimensions);
  }

  final long[] dimensions;

  private VectorClock(int dimensions) {
    this.dimensions = new long[dimensions];
    for (int i = 0; i < dimensions; i++) {
      this.dimensions[i] = Long.MIN_VALUE;
    }
  }

  public void update(int dimension, long stamp) {
    if (dimensions[dimension] < stamp) {
      dimensions[dimension] = stamp;
    }
  }

  public long getStamp() {
    long ret = Long.MAX_VALUE;
    for (long t : dimensions) {
      if (t < ret) {
        ret = t;
      }
    }
    return ret;
  }

}
