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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import java.time.Duration;
import java.util.Comparator;
import java.util.PriorityQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogObservers {

  /**
   * Create observer that observes data in event time order.
   *
   * <p>Data are first buffered into temporal buffer and hold until watermark - allowed lateness.
   *
   * @param upstream the upstream observer that observes ordered data
   * @param allowedLateness mixture of event time and processing time lateness
   * @return the observer to use for {@link CommitLogReader#observe(String, LogObserver)}
   */
  public static LogObserver withSortBuffer(LogObserver upstream, Duration allowedLateness) {

    return withSortBuffer(
        upstream,
        allowedLateness,
        el -> log.warn("Element {} dropped due to allowed lateness {}", el, allowedLateness));
  }

  /**
   * Create observer that observes data in event time order.
   *
   * <p>Data are first buffered into temporal buffer and hold until watermark - allowed lateness.
   *
   * @param upstream the upstream observer that observes ordered data
   * @param allowedLateness mixture of event time and processing time lateness
   * @param latecomerConsumer consumer of data that had to be dropped due to allowed lateness *
   * @return the observer to use for {@link CommitLogReader#observe(String, LogObserver)}
   */
  public static LogObserver withSortBuffer(
      LogObserver upstream, Duration allowedLateness, Consumer<StreamElement> latecomerConsumer) {

    PriorityQueue<Pair<StreamElement, OnNextContext>> queue =
        new PriorityQueue<>(Comparator.comparing(p -> p.getFirst().getStamp()));
    final long allowedLatenessMs = allowedLateness.toMillis();
    return new LogObserver() {

      @Override
      public void onCompleted() {
        drainQueue(Long.MAX_VALUE);
        upstream.onCompleted();
      }

      @Override
      public void onCancelled() {
        queue.clear();
        upstream.onCancelled();
      }

      @Override
      public boolean onError(Throwable error) {
        return upstream.onError(error);
      }

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        long watermark = context.getWatermark();
        if (context.getWatermark() - allowedLatenessMs <= ingest.getStamp()) {
          queue.add(Pair.of(ingest, context));
        } else {
          latecomerConsumer.accept(ingest);
          context.confirm();
        }
        return drainQueue(watermark - allowedLatenessMs);
      }

      @Override
      public void onRepartition(OnRepartitionContext context) {
        queue.clear();
        upstream.onRepartition(context);
      }

      @Override
      public void onIdle(OnIdleContext context) {
        drainQueue(context.getWatermark() - allowedLatenessMs);
        upstream.onIdle(context);
      }

      private boolean drainQueue(long maxTimestamp) {
        boolean cont = true;
        while (!queue.isEmpty() && cont) {
          cont = false;
          if (queue.peek().getFirst().getStamp() < maxTimestamp) {
            Pair<StreamElement, OnNextContext> p = queue.poll();
            if (!upstream.onNext(p.getFirst(), p.getSecond())) {
              queue.clear();
              return false;
            }
            cont = true;
          }
        }
        return true;
      }
    };
  }

  private LogObservers() {}
}
