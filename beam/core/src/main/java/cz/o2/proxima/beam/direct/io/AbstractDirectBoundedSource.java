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
package cz.o2.proxima.beam.direct.io;

import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/** Abstract super class for bounded sources. */
abstract class AbstractDirectBoundedSource extends BoundedSource<StreamElement> {

  private static final long serialVersionUID = 1L;

  final RepositoryFactory factory;

  AbstractDirectBoundedSource(RepositoryFactory factory) {
    this.factory = factory;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return -1L;
  }

  @Override
  public Coder<StreamElement> getOutputCoder() {
    return StreamElementCoder.of(factory);
  }
}
