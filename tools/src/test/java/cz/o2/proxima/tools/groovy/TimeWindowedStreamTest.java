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
package cz.o2.proxima.tools.groovy;

import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.executor.local.WatermarkTriggerScheduler;
import groovy.lang.Closure;

/**
 * Test suite for {@link TimeWindowedStream}.
 */
public class TimeWindowedStreamTest extends AbstractWindowedStreamTest {

  @SuppressWarnings("unchecked")
  @Override
  <T, W extends Windowing<T, ?>> WindowedStream<T, W> intoSingleWindow(Stream<T> stream) {
    return (WindowedStream) stream.assignEventTime(new Closure<Long>(this) {
      @Override
      public Long call(Object argument) {
        return 1L;
      }

    }).timeWindow(1000L);
  }

  @Override
  LocalExecutor executor() {
    return super.executor()
        .setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler(100L));
  }

}
