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

import java.util.Arrays;
import java.util.Random;

/**
 * An approximation of 1st, 10th, 30th, 50th, 70th, 90th and 99th percentile.
 */
public class ApproxPercentileMetric
    extends Metric<Stats>
    implements ApproxPercentileMetricMXBean {

  static final int SUBSAMPLE_SIZE = 500;

  public static ApproxPercentileMetric of(String group, String name) {
    return new ApproxPercentileMetric(group, name);
  }

  final Random random = new Random();
  final double[] capture = new double[SUBSAMPLE_SIZE];
  final int[] generations = new int[SUBSAMPLE_SIZE];
  int subsamples = 0;

  ApproxPercentileMetric(String group, String name) {
    super(group, name);
  }


  @Override
  public void increment(double d) {
    if (subsamples < SUBSAMPLE_SIZE) {
      // capture all data elements, until the array is full
      synchronized (this) {
        generations[subsamples] = 1;
        capture[subsamples++] = d;
        if (subsamples == SUBSAMPLE_SIZE) {
          Arrays.sort(capture);
        }
      }
    } else {
      synchronized (this) {
        int remove = random.nextInt(capture.length);
        double removed = capture[remove];
        double w = 1.0 / generations[remove];
        d = (d + removed * w) / (1 + w);
        int search = Arrays.binarySearch(capture, d);
        int insert = search;
        if (search < 0) {
          insert = - (search + 1);
        }
        if (insert < remove) {
          System.arraycopy(capture, insert, capture, insert + 1, remove - insert);
        } else if (insert != remove) {
          insert--;
          int toMove = insert - remove;
          if (toMove > 0) {
            System.arraycopy(capture, remove + 1, capture, remove, toMove);
          }
        }
        capture[insert] = d;
        generations[insert] = 0;
      }
      for (int i = 0; i < SUBSAMPLE_SIZE; i++) {
        generations[i]++;
      }
    }
  }


  @Override
  public synchronized Stats getValue() {
    // calculate the stastics from the capture array
    if (subsamples >= SUBSAMPLE_SIZE) {
      return new Stats(new double[] {
        capture[5],
        capture[50],
        capture[150],
        capture[250],
        capture[350],
        capture[450],
        capture[495]
      });
    }
    // not yet properly sampled
    // we need to copy the array and sort it
    if (subsamples > 50) {
      double[] sorted = Arrays.copyOf(capture, subsamples);
      Arrays.sort(sorted);
      int ratio = sorted.length / 10;
      return new Stats(new double[] {
          sorted[ratio / 10],
          sorted[ratio],
          sorted[3 * ratio],
          sorted[5 * ratio],
          sorted[7 * ratio],
          sorted[9 * ratio],
          sorted[99 * ratio / 10]
      });
    }
    // zeros, don't mess the stats with too fuzzy data
    return new Stats(new double[7]);
  }

  @Override
  public synchronized void reset() {
    subsamples = 0;
    for (int i = 0; i < capture.length; i++) {
      capture[i] = 0;
    }
  }

}
