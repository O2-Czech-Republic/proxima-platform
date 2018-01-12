/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.executor.Executor;
import java.time.Duration;

/**
 * A stream that is windowed by time.
 */
class TimeWindowedStream<T> extends WindowedStream<T, Windowing> {

  final long millis;
  final long slide;

  @SuppressWarnings("unchecked")
  TimeWindowedStream(
      Executor executor, DatasetBuilder<T> dataset, long millis, long slide,
      Runnable terminatingOperationCall) {

    super(
        executor, dataset,
        (Windowing)  (slide > 0
            ? TimeSliding.of(Duration.ofMillis(millis), Duration.ofMillis(slide))
            : Time.of(Duration.ofMillis(millis))),
        terminatingOperationCall,
        (w, d) -> {
          if (slide > 0) {
            TimeSliding s = (TimeSliding) w;
            // FIXME: https://github.com/seznam/euphoria/issues/245
            throw new UnsupportedOperationException("Euphoria issue #245");
          } else {
            Time t = (Time) w;
            t.earlyTriggering(d);
          }
          return w;
        });

    this.millis = millis;
    this.slide = slide;
  }

  TimeWindowedStream(
      Executor executor, DatasetBuilder<T> dataset, long millis,
      Runnable terminatingOperationCall) {

    this(
        executor, dataset, millis, -1L,
        terminatingOperationCall);
  }

  @Override
  <X> WindowedStream<X, Windowing> descendant(DatasetBuilder<X> dataset) {
    return new TimeWindowedStream<>(
        executor, dataset, millis, slide,
        terminatingOperationCall);
  }

}
