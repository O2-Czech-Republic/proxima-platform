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

import com.google.auto.service.AutoService;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.tools.groovy.StreamProvider;
import cz.o2.proxima.util.ExceptionUtils;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration.Builder;

@Slf4j
@AutoService(StreamProvider.class)
public class TestBeamStreamProvider extends BeamStreamProvider {

  static Class<? extends PipelineRunner<?>> runner = DirectRunner.class;

  private Runnable onClose = null;
  private boolean clusterRunning = false;

  /**
   * Create factory to be used for pipeline creation.
   *
   * @return the factory
   */
  @Override
  protected UnaryFunction<PipelineOptions, Pipeline> getCreatePipelineFromOpts() {
    return opts -> {
      opts.setRunner(runner);
      if (runner.getSimpleName().equals("FlinkRunner")) {
        prepareFlinkEnvironment(opts.as(FlinkPipelineOptions.class));
      }
      return Pipeline.create(opts);
    };
  }

  @Override
  public void close() {
    super.close();
    Optional.ofNullable(onClose).ifPresent(Runnable::run);
  }

  private void prepareFlinkEnvironment(FlinkPipelineOptions opts) {
    if (!clusterRunning) {
      Configuration conf = new Configuration();
      conf.setLong("taskmanager.memory.process.size", 10 * 1024 * 1024L);
      MiniClusterConfiguration clusterConf =
          new Builder()
              .setNumTaskManagers(1)
              .setNumSlotsPerTaskManager(1)
              .setConfiguration(conf)
              .build();
      MiniCluster cluster = new MiniCluster(clusterConf);
      clusterRunning = true;
      ExceptionUtils.unchecked(cluster::start);
      opts.setFlinkMaster(
          ExceptionUtils.uncheckedFactory(cluster.getRestAddress()::get).toString());
      onClose =
          () -> {
            log.info("Closing Flink MiniCluster");
            ExceptionUtils.unchecked(cluster::close);
            clusterRunning = false;
          };
    }
  }
}
