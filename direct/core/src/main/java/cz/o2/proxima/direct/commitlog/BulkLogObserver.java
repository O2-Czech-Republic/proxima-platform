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

import cz.o2.proxima.annotations.Stable;
import java.util.List;

/**
 * Observer of a {@code CommitLog} that commits offsets in bulk fashion
 * and not one-by-one.
 * This observer provides access only to the data that was not yet consumed
 * by the named consumer. The consumer maintains position in the commit log
 * so if it crashes, then it will continue consuming from the committed position.
 * However, it is not guaranteed that a message will not be delivered multiple
 * times, nor it is guaranteed that messages will be delivered in-order.
 *
 * Implementation has to override either of `onNext` methods.
 */
@Stable
public interface BulkLogObserver extends LogObserver {

  /**
   * Called when the bulk processing is restarted from last committed position.
   * @param offsets the offsets at which the processing starts for each partition
   */
  default void onRestart(List<Offset> offsets) {

  }

}
