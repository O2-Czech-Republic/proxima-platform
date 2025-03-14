/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core.commitlog;

import cz.o2.proxima.core.annotations.Stable;
import cz.o2.proxima.core.storage.Partition;
import java.io.Serializable;

/** Interface for implementations and their offset. */
@Stable
public interface Offset extends Serializable {

  /**
   * Which partition is this offset for
   *
   * @return partition of offset
   */
  Partition getPartition();

  /**
   * Retrieve watermark associated with this offset.
   *
   * @return watermark associated with this offset
   */
  long getWatermark();
}
