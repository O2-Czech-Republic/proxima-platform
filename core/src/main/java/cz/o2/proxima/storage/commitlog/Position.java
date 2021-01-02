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
package cz.o2.proxima.storage.commitlog;

import cz.o2.proxima.annotations.Stable;

/** An enum specifying the position in the commit log to start reading from. */
@Stable
public enum Position {

  /**
   * Read the commit log from the current data actually pushed to the log or the currently committed
   * position.
   */
  NEWEST,

  /** Read from given offsets (current). */
  CURRENT,

  /** Read the commit log from the oldest data available. */
  OLDEST;
}
