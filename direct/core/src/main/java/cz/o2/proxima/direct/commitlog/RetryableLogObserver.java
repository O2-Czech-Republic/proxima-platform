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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.storage.StreamElement;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * {@code LogObserver} which is able to retry the observation on error. The number of retries is
 * configurable.
 */
@Stable
@Slf4j
public class RetryableLogObserver implements LogObserver {

  /**
   * Create online retryable log observer.
   *
   * @param name name of the consumer
   * @param numRetries number of allowed successive failures
   * @param observer observer of data
   * @return the observer
   */
  public static RetryableLogObserver of(String name, int numRetries, LogObserver observer) {
    return new RetryableLogObserver(name, numRetries, observer);
  }

  /** Maximal number of retries. */
  @Getter private final int maxRetries;
  /** Name of the consumer. */
  @Getter private final String name;
  /** Current number of failures in a row. */
  private int numFailures;
  /** Underlying log observer. */
  private final LogObserver delegate;

  private RetryableLogObserver(String name, int maxRetries, LogObserver delegate) {
    this.maxRetries = maxRetries;
    this.name = name;
    this.delegate = delegate;
  }

  @Override
  public final boolean onNext(StreamElement ingest, OnNextContext context) {
    boolean ret = delegate.onNext(ingest, context);
    numFailures = 0;
    return ret;
  }

  @Override
  public boolean onError(Throwable throwable) {
    if (delegate.onError(throwable)) {
      numFailures++;
      log.error(
          "Error in observer {}, retry {} out of {}", name, numFailures, maxRetries, throwable);
      return numFailures <= maxRetries;
    }
    log.error("Error in observer {} (non-retryable)", name, throwable);
    return false;
  }

  @Override
  public void onCompleted() {
    delegate.onCompleted();
  }

  @Override
  public void onCancelled() {
    delegate.onCancelled();
  }

  @Override
  public void onRepartition(OnRepartitionContext context) {
    delegate.onRepartition(context);
  }

  @Override
  public void onIdle(OnIdleContext context) {
    delegate.onIdle(context);
  }
}
