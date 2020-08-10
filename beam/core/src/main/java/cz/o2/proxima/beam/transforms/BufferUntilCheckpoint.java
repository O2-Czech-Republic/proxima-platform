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
package cz.o2.proxima.beam.transforms;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Identity transformation which ensures exactly-once output. Generally an input elements are
 * buffered and are outputs only when checkpoint is stored successfully.
 *
 * <p>Use this transformation when output is not stable (e.g. random values) or when transformation
 * generates side effects which are not idempotent. For details @see <a
 * href="https://docs.google.com/document/d/117yRKbbcEdm3eIKB_26BHOJGmHSZl1YNoF0RqWGtqAM">Beam
 * design document</a>
 *
 * @param <T> Element type
 */
public class BufferUntilCheckpoint<T> extends DoFn<T, T> {
  @RequiresStableInput
  @ProcessElement
  public void processElement(@Element T input, OutputReceiver<T> out) {
    out.output(input);
  }
}
