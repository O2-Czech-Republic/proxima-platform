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
package cz.o2.proxima.beam.tools.groovy;

import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.tools.groovy.GroovyEnv;
import cz.o2.proxima.tools.groovy.GroovyEnvTest;
import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.PipelineRunner;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Test {@link GroovyEnv} using beam. */
@RunWith(Parameterized.class)
public class BeamGroovyEnvTest extends GroovyEnvTest {

  @Parameters
  public static Collection<Class<? extends PipelineRunner>> parameters() {
    return Arrays.asList(DirectRunner.class, FlinkRunner.class, SparkRunner.class);
  }

  public BeamGroovyEnvTest(Class<? extends PipelineRunner> runner) {
    TestBeamStreamProvider.runner = runner;
  }

  private final DirectDataOperator direct = getRepo().getOrCreateOperator(DirectDataOperator.class);

  @Override
  protected void write(StreamElement element) {
    direct
        .getWriter(element.getAttributeDescriptor())
        .orElseThrow(
            () ->
                new IllegalStateException("Missing writer for " + element.getAttributeDescriptor()))
        .write(element, (succ, exc) -> {});
  }
}
