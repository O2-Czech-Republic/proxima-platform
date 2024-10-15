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
package cz.o2.proxima.beam.tools.groovy;

import static cz.o2.proxima.beam.util.RunnerUtils.injectJarIntoContextClassLoader;
import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.tools.groovy.WindowedStream;
import cz.o2.proxima.tools.groovy.util.Closures;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.Test;

/** Test suite for {@link BeamStreamProvider}. */
public class BeamStreamProviderTest {

  public static class Registrar implements BeamStreamProvider.RunnerRegistrar {

    @Override
    public void apply(PipelineOptions opts) {
      // nop
    }
  }

  private final Repository repo = Repository.ofTest(ConfigFactory.defaultApplication());

  @Test
  public void testRunnerRegistrarParsing() {
    try (BeamStreamProvider.Default provider = new BeamStreamProvider.Default()) {
      provider.init(
          repo,
          new String[] {
            "--runner=flink",
            "--runnerRegistrar=" + Registrar.class.getName(),
            "--checkpointingInterval=10000"
          });
      assertNotNull(provider.getPipelineOptionsFactory());
      PipelineOptions options = provider.getPipelineOptionsFactory().get();
      assertNotNull(options);
      assertEquals(
          10000L, (long) options.as(FlinkPipelineOptions.class).getCheckpointingInterval());
      assertEquals("FlinkRunner", options.getRunner().getSimpleName());
      assertEquals(1, provider.getRegistrars().size());
      assertEquals(2, provider.getArgs().length);
    }
  }

  @Test
  public void testRunnerUnknownRunnerJarInject() {
    try (BeamStreamProvider.Default provider = new BeamStreamProvider.Default()) {
      provider.init(
          repo,
          new String[] {
            "--runner=TestFlinkRunner",
            "--runnerRegistrar=" + Registrar.class.getName(),
            "--checkpointingInterval=10000"
          });
      provider.createUdfJarAndRegisterToPipeline(provider.getPipelineOptionsFactory().get());
      assertTrue(Thread.currentThread().getContextClassLoader() instanceof URLClassLoader);
    }
  }

  @Test
  public void testInjectPathToClassloader() throws IOException {
    File f = File.createTempFile("dummy", ".tmp");
    injectJarIntoContextClassLoader(Collections.singletonList(f));
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    assertTrue(loader instanceof URLClassLoader);
  }

  @Test
  public void testImpulses() {
    try (BeamStreamProvider.Default provider = new BeamStreamProvider.Default()) {
      provider.init(
          repo,
          new String[] {
            "--runner=TestFlinkRunner",
            "--runnerRegistrar=" + Registrar.class.getName(),
            "--checkpointingInterval=10000"
          });
      WindowedStream<Long> periodicImpulse =
          provider.periodicImpulse(Closures.from(this, () -> 1L), 1000L);
      WindowedStream<Long> impulse = provider.impulse(Closures.from(this, () -> 1L));
      assertNotNull(periodicImpulse);
      assertNotNull(impulse);
      List<Long> collected = impulse.collect();
      assertEquals(Collections.singletonList(1L), collected);
    }
  }
}
