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
package cz.o2.proxima.storage.watermark;

import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.time.WatermarkSupplier;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;

/**
 * A tracker of global watermark progress among multiple (distributed) processes.
 *
 * <p>A {@link GlobalWatermarkTracker} consists of the following:
 *
 * <ol>
 *   <li>Name of the tracker. The name MUST uniquely identify the tracker among all possible global
 *       trackers. The name also serves as grouping identifier for calculation of the value of
 *       global watermark.
 *   <li>Predefined list of <i>processes</i>.
 * </ol>
 */
@Evolving
public interface GlobalWatermarkTracker extends WatermarkSupplier, Closeable {

  /**
   * Retrieve name of this tracker.
   *
   * @return name of the tracker
   */
  String getName();

  /**
   * Configure the tracker using given configuration.
   *
   * @param cfg the (scoped) configuration map
   */
  void setup(Map<String, Object> cfg);

  /**
   * Setup parallel consumers. This SHOULD be used once when the tracker is constructed to setup
   * initial names and watermarks of consumers. Note that consumers might be added during runtime,
   * but <b>their added watermark cannot move the watermark back in time</b>.
   *
   * @param initialWatermarks map of process name to the initial watermark
   */
  void initWatermarks(Map<String, Long> initialWatermarks);

  /**
   * Update watermark of given process. This call MAY add a new process, which was not part of
   * {@link #initWatermarks}. This is asynchronous operation. Users can wait for the completion
   * using the returned {@link CompletableFuture}.
   *
   * @param processName name of the process
   * @param currentWatermark current processing watermark of the process
   * @return {@link CompletableFuture} to be able to wait for result being persisted
   */
  CompletableFuture<Void> update(String processName, long currentWatermark);

  /**
   * Remove given process from the tracker. The watermark of the process (if any) will no longer
   * hold the global watermark. This is asynchronous operation. Users can wait for the completion
   * using the returned {@link CompletableFuture}.
   *
   * <p>Note that this is semantically equivalent to call to {@link #update}(name,
   * Instant.ofEpochMilli(Long.MAX_VALUE))
   *
   * @param name name of the process to remove
   * @return {@link CompletableFuture} to be able to wait for result being persisted
   */
  default CompletableFuture<Void> finished(String name) {
    return update(name, Long.MAX_VALUE);
  }

  /**
   * Retrieve global watermark tracked by this tracker.
   *
   * @param processName name of process querying the global watermark
   * @param currentWatermark current watermark of process querying the watermark
   * @return the global watermark
   */
  long getGlobalWatermark(@Nullable String processName, long currentWatermark);

  @Override
  default long getWatermark() {
    return getGlobalWatermark(null, Long.MAX_VALUE);
  }
}
