/**
 * Copyright 2017-${Year} O2 Czech Republic, a.s.
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

import cz.o2.proxima.functional.UnaryFunction;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.options.PipelineOptions;

public class TestBeamStreamProvider extends BeamStreamProvider {

  static Class<? extends PipelineRunner> runner = DirectRunner.class;

  /**
   * Create factory to be used for pipeline creation.
   *
   * @return the factory
   */
  @SuppressWarnings("unchecked")
  @Override
  protected UnaryFunction<PipelineOptions, Pipeline> getCreatePipelineFromOpts() {
    return opts -> {
      opts.setRunner((Class) runner);
      Pipeline ret = Pipeline.create(opts);
      // register kryo for object
      // remove this as soon as we figure out how to correctly
      // pass type information around in groovy DSL
      ret.getCoderRegistry().registerCoderForClass(Object.class, KryoCoder.of());
      return ret;
    };
  }
}
