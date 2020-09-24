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
package cz.o2.proxima.direct.core;

import cz.o2.proxima.annotations.Stable;

/** Callback for write and commit log operations. */
@Stable
@FunctionalInterface
public interface CommitCallback {

  static CommitCallback noop() {
    return (success, error) -> {};
  }

  /**
   * Commit the ingest process.
   *
   * @param success {@code true} is the write was successful, {@code false} otherwise
   * @param error the error that was throws during the processing.
   */
  void commit(boolean success, Throwable error);
}
