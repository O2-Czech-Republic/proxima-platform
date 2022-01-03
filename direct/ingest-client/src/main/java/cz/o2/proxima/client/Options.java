/**
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.client;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;

/** Client related options. */
public class Options {

  @Getter @Setter
  /** How often to flush the data in microseconds. */
  private int flushUsec = 50000;

  @Getter @Setter
  /** Maximal size of the bulk before being sent even before flushUsec. */
  private int maxFlushRecords = 1000;

  @Getter @Setter
  /** Executor to use for grpc. */
  private Executor executor =
      new ThreadPoolExecutor(
          Runtime.getRuntime().availableProcessors(),
          5 * Runtime.getRuntime().availableProcessors(),
          5,
          TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(5 * Runtime.getRuntime().availableProcessors()));

  @Getter @Setter
  /** Maximal count of inflight records. */
  private int maxInflightRequests = 500000;
}
