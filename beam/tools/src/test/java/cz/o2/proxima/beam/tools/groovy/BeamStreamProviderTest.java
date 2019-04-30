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

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.Repository;
import org.apache.beam.sdk.options.PipelineOptions;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test suite for {@link BeamStreamProvider}.
 */
public class BeamStreamProviderTest {

  public static class Registrar implements BeamStreamProvider.RunnerRegistrar {

    @Override
    public void apply(PipelineOptions opts) {
      // nop
    }

  }

  private final Repository repo = Repository.of(ConfigFactory.defaultApplication());

  @Test
  public void testRunnerRegistrarParsing() {
    try (BeamStreamProvider.Default provider = new BeamStreamProvider.Default()) {
      provider.init(repo, new String[] {
          "--runner=flink",
          "--checkpointingInterval=10000",
          "--runnerRegistrar=" + Registrar.class.getName()
      });
      assertNotNull(provider.getPipelineOptionsFactory());
      assertNotNull(provider.getPipelineOptionsFactory().apply());
      assertEquals(1, provider.getRegistrars().size());
      assertEquals(2, provider.getArgs().length);
    }
  }

}
