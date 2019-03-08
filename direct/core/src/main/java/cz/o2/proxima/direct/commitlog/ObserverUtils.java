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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.time.WatermarkSupplier;
import java.util.Collection;

/**
 * Various utilities for working with {@link LogObserver}.
 */
@Internal
public class ObserverUtils {

  public static LogObserver.OnNextContext asOnNextContext(
      LogObserver.OffsetCommitter committer,
      Partition partition,
      WatermarkSupplier watermarkSupplier) {

    return new LogObserver.OnNextContext() {

      @Override
      public LogObserver.OffsetCommitter committer() {
        return committer;
      }

      @Override
      public Partition getPartition() {
        return partition;
      }

      @Override
      public long getWatermark() {
        return watermarkSupplier.getWatermark();
      }

    };
  }

  public static LogObserver.OnRepartitionContext asRepartitionContext(
      Collection<Partition> assigned) {

    return () -> assigned;
  }

  private ObserverUtils() { }

}
