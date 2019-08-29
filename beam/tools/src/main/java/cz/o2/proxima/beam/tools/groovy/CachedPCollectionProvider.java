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
package cz.o2.proxima.beam.tools.groovy;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link PCollectionProvider} that caches the outcome of underlying
 * provider and returns it is it has been already materialized.
 */
public class CachedPCollectionProvider<T> implements PCollectionProvider<T> {

  private final PCollectionProvider<T> underlying;
  private transient PCollection<T> materialized;

  CachedPCollectionProvider(PCollectionProvider<T> underlying) {
    this.underlying = underlying;
  }

  @Override
  public PCollection<T> materialize(Pipeline pipeline) {
    if (materialized == null || materialized.getPipeline() != pipeline) {
      materialized = underlying.materialize(pipeline);
    }
    return materialized;
  }

}
