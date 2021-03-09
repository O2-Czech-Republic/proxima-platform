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

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.commitlog.LogObservers.TerminationStrategy;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.storage.StreamElement;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * {@code LogObserver} which is able to retry the observation on error. The number of retries is
 * configurable.
 */
@Internal
@Slf4j
class RetryableLogObserver implements LogObserver {

  /** Maximal number of retries. */
  @Getter private final int maxRetries;
  /** Name of the consumer. */
  @Getter private final String name;
  /** Consumer of error when retries exhausted. */
  UnaryFunction<Throwable, TerminationStrategy> onRetriesExhausted;
  /** {@code true} is we should retry {@link Error Errors}. */
  final boolean retryErrors;
  /** Current number of failures in a row. */
  private int numFailures;
  /** Underlying log observer. */
  private final LogObserver delegate;

  RetryableLogObserver(
      String name,
      int maxRetries,
      UnaryFunction<Throwable, TerminationStrategy> onRetriesExhausted,
      boolean retryErrors,
      LogObserver delegate) {

    this.maxRetries = maxRetries;
    this.name = name;
    this.onRetriesExhausted = onRetriesExhausted;
    this.retryErrors = retryErrors;
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
    if (delegate.onError(throwable)
        && (retryErrors || !(throwable instanceof Error))
        && ++numFailures <= maxRetries) {

      log.error(
          "Error in observer {}, retry {} out of {}", name, numFailures, maxRetries, throwable);
      return true;
    }
    log.error("Error in observer {} (non-retryable)", name, throwable);
    TerminationStrategy strategy = onRetriesExhausted.apply(throwable);
    return handleThrowableWithStrategy(name, throwable, strategy);
  }

  @VisibleForTesting
  static boolean handleThrowableWithStrategy(
      String name, Throwable throwable, TerminationStrategy strategy) {

    switch (strategy) {
      case EXIT:
        log.error("Exception caught processing {}. Exiting.", name, throwable);
        System.exit(1);
      case STOP_PROCESSING:
        log.error(
            "Exception caught processing {}. Terminating consumption as requested.",
            name,
            throwable);
        return false;
      case RETHROW:
        if (throwable instanceof Error) {
          throw (Error) throwable;
        }
        if (throwable instanceof RuntimeException) {
          throw (RuntimeException) throwable;
        }
        throw new IllegalStateException("Retries exhausted retrying observer " + name, throwable);
    }
    throw new IllegalStateException(
        String.format("Unknown TerminationStrategy %s in %s", strategy, name), throwable);
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
