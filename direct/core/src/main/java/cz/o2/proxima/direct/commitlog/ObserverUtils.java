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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.time.WatermarkSupplier;
import java.util.Collection;

/** Various utilities for working with {@link CommitLogObserver}. */
@Internal
public class ObserverUtils {

  public static CommitLogObserver.OnNextContext asOnNextContext(
      CommitLogObserver.OffsetCommitter committer, Offset offset) {

    return new CommitLogObserver.OnNextContext() {

      private static final long serialVersionUID = 1L;

      @Override
      public CommitLogObserver.OffsetCommitter committer() {
        return committer;
      }

      @Override
      public Partition getPartition() {
        return offset.getPartition();
      }

      @Override
      public Offset getOffset() {
        return offset;
      }

      @Override
      public long getWatermark() {
        return offset.getWatermark();
      }
    };
  }

  public static CommitLogObserver.OnRepartitionContext asRepartitionContext(
      Collection<Partition> assigned) {

    return () -> assigned;
  }

  public static CommitLogObserver.OnIdleContext asOnIdleContext(WatermarkSupplier supplier) {
    return supplier::getWatermark;
  }

  private ObserverUtils() {}
}
