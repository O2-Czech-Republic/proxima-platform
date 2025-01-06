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
package cz.o2.proxima.direct.io.blob;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Internal
public class RetryStrategy implements Serializable {

  private final int initialRetryDelay;
  private final int maxRetryDelay;
  private final Set<Class<? extends Exception>> retryableException = new HashSet<>();

  public RetryStrategy(int initialRetryDelay, int maxRetryDelay) {
    Preconditions.checkArgument(
        initialRetryDelay < maxRetryDelay / 2,
        "Max retry delay must be at least double of initial delay, got %s and %s",
        initialRetryDelay,
        maxRetryDelay);
    this.initialRetryDelay = initialRetryDelay;
    this.maxRetryDelay = maxRetryDelay;
  }

  public RetryStrategy withRetryableException(Class<? extends Exception> ex) {
    retryableException.add(ex);
    return this;
  }

  public void retry(Runnable what) {
    retry(
        () -> {
          what.run();
          return null;
        });
  }

  public <T> T retry(Factory<T> what) {
    int delay = initialRetryDelay;
    while (true) {
      try {
        return what.apply();
      } catch (Exception ex) {
        boolean rethrow = true;
        if (retryableException.contains(ex.getClass())) {
          boolean shouldRetry = delay <= maxRetryDelay;
          if (shouldRetry) {
            log.warn(
                "Exception while communicating with cloud storage. Retrying after {} ms",
                delay,
                ex);
            long effectiveDelay = delay;
            ExceptionUtils.unchecked(() -> TimeUnit.MILLISECONDS.sleep(effectiveDelay));
            delay *= 2;
            rethrow = false;
          }
        }
        if (rethrow) {
          throw ex;
        }
      }
    }
  }
}
