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
package cz.o2.proxima.time;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;

/** Vector clock implementation. */
@Internal
public interface VectorClock extends WatermarkSupplier {

  /**
   * Create new instance of VectorClock.
   *
   * @param dimensions dimensions of the clock
   * @return new instance
   */
  public static VectorClock of(int dimensions) {
    return of(dimensions, Long.MIN_VALUE);
  }

  /**
   * Create new instance of VectorClock.
   *
   * @param dimensions dimensions of the clock
   * @param initialStamp timestamp to initialize all dimensions to
   * @return new instance
   */
  public static VectorClock of(int dimensions, long initialStamp) {
    if (dimensions > 0) {
      return new VectorClockImpl(dimensions, initialStamp);
    }
    return new ProcessingTimeClock();
  }

  final class VectorClockImpl implements VectorClock {

    private static final long serialVersionUID = 1L;

    final long[] dimensions;

    private VectorClockImpl(int dimensions, long initialStamp) {
      Preconditions.checkArgument(dimensions > 0, "Number of dimensions must be positive");
      this.dimensions = new long[dimensions];
      for (int i = 0; i < dimensions; i++) {
        this.dimensions[i] = initialStamp;
      }
    }

    @Override
    public void update(int dimension, long stamp) {
      if (dimensions[dimension] < stamp) {
        dimensions[dimension] = stamp;
      }
    }

    @Override
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

  // a fallback implementation that is used when supplied dimensions
  // is empty
  final class ProcessingTimeClock implements VectorClock {

    private static final long serialVersionUID = 1L;

    @Override
    public void update(int dimension, long stamp) {
      // nop
    }

    @Override
    public long getStamp() {
      return getProcessingStamp();
    }
  }

  void update(int dimension, long stamp);

  long getStamp();

  default long getProcessingStamp() {
    return System.currentTimeMillis();
  }

  @Override
  public default long getWatermark() {
    return getStamp();
  }
}
