/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.server;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A configured backoff retry policy.
 */
public class BackoffRetryPolicy implements RetryPolicy {
  
  private static final Logger LOG = LoggerFactory.getLogger(BackoffRetryPolicy.class);
  
  final int maxRetries;
  final long startTimeout;
  final ScheduledExecutorService executor;

  class Task {
    @Getter
    private final AsyncRunnable runnable;
    @Getter
    private int numRetries = 0;
    @Getter
    private long timeout = startTimeout;
    Task(AsyncRunnable runnable) {
      this.runnable = runnable;
    }
    void run(Consumer<Boolean> callback) {
      runnable.run(exc -> {
        if (exc == null) {
          callback.accept(true);
        } else if (numRetries ++ < maxRetries) {
          LOG.warn("Failed to run task {}", runnable, exc);
          executor.schedule(() -> this.run(callback), timeout, TimeUnit.MILLISECONDS);
        } else {
          callback.accept(false);
        }
      });
    }
  }
  
  public BackoffRetryPolicy(
      ScheduledExecutorService executor,
      int maxRetires, long startTimeout) {

    this.executor = executor;
    this.maxRetries = maxRetires;
    this.startTimeout = startTimeout;
  }

  @Override
  public void retry(AsyncRunnable what, Consumer<Boolean> callback) {
    new Task(what).run(callback);
  }

}
