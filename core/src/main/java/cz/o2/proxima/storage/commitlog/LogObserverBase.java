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

package cz.o2.proxima.storage.commitlog;

import java.io.Serializable;

/**
 * Base interface for bulk and online observers.
 */
public interface LogObserverBase extends AutoCloseable, Serializable {

  /**
   * Notify that the processing has gracefully ended.
   */
  default void onCompleted() {

  }

  /**
   * Notify that the processing has been canceled.
   */
  default void onCancelled() {

  }

  /**
   * Called to notify there was an error in the commit reader.
   */
  void onError(Throwable error);

}
