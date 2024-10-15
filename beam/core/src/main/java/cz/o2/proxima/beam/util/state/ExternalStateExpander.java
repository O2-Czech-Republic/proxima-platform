/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.util.state;

import cz.o2.proxima.beam.util.RunnerUtils;
import cz.o2.proxima.core.functional.UnaryFunction;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;

@Slf4j
public class ExternalStateExpander {

  /**
   * Expand the given @{link Pipeline} to support external state store and restore
   *
   * @param pipeline the Pipeline to expand
   * @param inputs transform to read inputs
   * @param stateWriteInstant the instant at which write of the last state occurred
   * @param nextFlushInstantFn function that returns instant of next flush from current time
   * @param stateSink transform to store outputs
   */
  public static Pipeline expand(
      Pipeline pipeline,
      PTransform<PBegin, PCollection<KV<String, StateValue>>> inputs,
      Instant stateWriteInstant,
      UnaryFunction<Instant, Instant> nextFlushInstantFn,
      PTransform<PCollection<KV<String, StateValue>>, PDone> stateSink)
      throws IOException {

    ExpandContext context =
        new ExpandContext(inputs, stateWriteInstant, nextFlushInstantFn, stateSink);
    Pipeline expanded = context.expand(pipeline);
    File dynamicJar = RunnerUtils.createJarFromDynamicClasses(context.getGeneratedClasses());
    RunnerUtils.registerToPipeline(
        expanded.getOptions(),
        expanded.getOptions().getRunner().getSimpleName(),
        Collections.singletonList(dynamicJar));
    return expanded;
  }

  // do not construct
  private ExternalStateExpander() {}
}
