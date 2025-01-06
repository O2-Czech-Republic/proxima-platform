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
package cz.o2.proxima.beam.tools.groovy;

import cz.o2.proxima.tools.groovy.WindowedStreamTest;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.PipelineRunner;

public class BeamWindowedStreamDirectTest extends WindowedStreamTest {

  public BeamWindowedStreamDirectTest() {
    this(DirectRunner.class);
  }

  protected BeamWindowedStreamDirectTest(Class<? extends PipelineRunner<?>> runner) {
    super(BeamStreamTest.provider(true, runner));
  }
}
