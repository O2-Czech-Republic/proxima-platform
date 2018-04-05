/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage.pubsub;

import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;

/**
 * Partitioner for {@link StreamElement}s.
 */
@FunctionalInterface
public interface Partitioner extends Serializable {

  /**
   * Retrieve partition for given element.
   * @param element the element to retrieve partition ID for
   * @return ID of partition (any integer)
   */
  int getPartition(StreamElement element);

}
