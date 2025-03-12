/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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

import com.google.auto.service.AutoService;
import java.util.Collections;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface ProximaPipelineOptions extends PipelineOptions {

  @AutoService(PipelineOptionsRegistrar.class)
  class Registrar implements PipelineOptionsRegistrar {

    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return Collections.singletonList(ProximaPipelineOptions.class);
    }
  }

  /** {@code false} to preserve UDF jar on exit. */
  @Default.Boolean(true)
  boolean getPreserveUDFJar();

  void setPreserveUDFJar(boolean preserve);

  /** Set directory where to store generated UDF jar(s). */
  @Nullable String getUdfJarDirPath();

  void setUdfJarDirPath(String path);

  /** Set delay for {@link cz.o2.proxima.beam.core.direct.io.BatchLogRead} in ms. */
  @Default.Long(0L)
  long getStartBatchReadDelayMs();

  void setStartBatchReadDelayMs(long readDelayMs);

  /** Enforce stable input for remote consumers in console */
  @Default.Boolean(true)
  boolean getEnforceStableInputForRemoteConsumer();

  void setEnforceStableInputForRemoteConsumer(boolean enforce);
}
