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
package cz.o2.proxima.beam.core;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.internal.AbstractDataAccessor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

/**
 * A data accessor for attribute families.
 */
@Internal
public interface DataAccessor extends AbstractDataAccessor {

  /**
   * Create {@link PCollection} for given attribute family.
   * @param pipeline pipeline to create {@link PCollection} in
   * @param position to read from
   * @param stopAtCurrent stop reading at current data
   * @param eventTime {@code true} to use event time
   * @return {@link PCollection} representing the commit log
   */
  PCollection<StreamElement> getCommitLog(
      Pipeline pipeline, Position position,
      boolean stopAtCurrent, boolean eventTime);

}
