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

import cz.o2.proxima.direct.commitlog.LogObservers.TerminationStrategy;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

public class RetryableLogObserverTest {

  @Test
  public void testRetryableException() {
    final int numRetries = 10;
    final AtomicReference<Throwable> exception = new AtomicReference<>();
    final RetryableLogObserver observer =
        new RetryableLogObserver(
            "test",
            numRetries,
            withStrategy(exception::set, TerminationStrategy.RETHROW),
            false,
            new LogObserver() {

              @Override
              public boolean onError(Throwable error) {
                // Retryable.
                return true;
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                return true;
              }
            });

    try {
      observer.onError(new OutOfMemoryError());
      Assert.fail("Should have thrown exception");
    } catch (OutOfMemoryError ex) {
      Assert.assertEquals(ex, exception.get());
    }

    // Retry for max-number of times.
    for (int i = 0; i < numRetries; i++) {
      Assert.assertTrue(observer.onError(new Exception("Test.")));
    }

    // Out of retries.
    try {
      observer.onError(new Exception("Test."));
      Assert.fail("Should have thrown exception");
    } catch (IllegalStateException ex) {
      Assert.assertEquals(ex.getCause(), exception.get());
    }

    // Failure counter restarts after element is successfully processed.
    observer.onNext(null, null);

    // Retry for max-number of times.
    for (int i = 0; i < numRetries; i++) {
      Assert.assertTrue(observer.onError(new Exception("Test.")));
    }

    // Out of retries.
    try {
      observer.onError(new Exception("Test."));
      Assert.fail("Should have thrown exception");
    } catch (IllegalStateException ex) {
      Assert.assertEquals(ex.getCause(), exception.get());
    }
  }

  @Test
  public void testNonRetryableException() {
    final int numRetries = 10;
    final AtomicReference<Throwable> exception = new AtomicReference<>();
    final RetryableLogObserver observer =
        new RetryableLogObserver(
            "test",
            numRetries,
            withStrategy(exception::set, TerminationStrategy.RETHROW),
            false,
            new LogObserver() {

              @Override
              public boolean onError(Throwable error) {
                // Non-Retryable.
                return false;
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                throw new UnsupportedOperationException("Not implemented.");
              }
            });
    try {
      observer.onError(new OutOfMemoryError());
      Assert.fail("Should have thrown exception");
    } catch (OutOfMemoryError ex) {
      Assert.assertEquals(ex, exception.get());
    }
    try {
      observer.onError(new Exception("Test."));
      Assert.fail("Should have thrown exception");
    } catch (IllegalStateException ex) {
      Assert.assertEquals(ex.getCause(), exception.get());
    }
  }

  @Test
  public void testNonRetryableExceptionWithStopProcessing() {
    final int numRetries = 10;
    final AtomicReference<Throwable> exception = new AtomicReference<>();
    final RetryableLogObserver observer =
        new RetryableLogObserver(
            "test",
            numRetries,
            withStrategy(exception::set, TerminationStrategy.STOP_PROCESSING),
            false,
            new LogObserver() {

              @Override
              public boolean onError(Throwable error) {
                // Non-Retryable.
                return false;
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                throw new UnsupportedOperationException("Not implemented.");
              }
            });

    Assert.assertFalse(observer.onError(new OutOfMemoryError()));
    Assert.assertFalse(observer.onError(new Exception("Test.")));
  }

  @Test
  public void testRetryableError() {
    final int numRetries = 10;
    final AtomicReference<Throwable> exception = new AtomicReference<>();
    final RetryableLogObserver observer =
        new RetryableLogObserver(
            "test",
            numRetries,
            withStrategy(exception::set, TerminationStrategy.RETHROW),
            true,
            new LogObserver() {

              @Override
              public boolean onError(Throwable error) {
                // Retryable.
                return true;
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                return true;
              }
            });

    // Retry for max-number of times.
    for (int i = 0; i < numRetries; i++) {
      Assert.assertTrue(observer.onError(new OutOfMemoryError("Test.")));
    }

    // Out of retries.
    try {
      observer.onError(new OutOfMemoryError("Test."));
      Assert.fail("Should have thrown exception");
    } catch (OutOfMemoryError ex) {
      Assert.assertEquals(ex, exception.get());
    }

    // Failure counter restarts after element is successfully processed.
    observer.onNext(null, null);

    // Retry for max-number of times.
    for (int i = 0; i < numRetries; i++) {
      Assert.assertTrue(observer.onError(new OutOfMemoryError("Test.")));
    }

    // Out of retries.
    try {
      observer.onError(new OutOfMemoryError("Test."));
      Assert.fail("Should have thrown exception");
    } catch (OutOfMemoryError ex) {
      Assert.assertEquals(ex, exception.get());
    }
  }

  @Test
  public void testRetryableErrorWithStopProcessing() {
    final int numRetries = 10;
    final AtomicReference<Throwable> exception = new AtomicReference<>();
    final RetryableLogObserver observer =
        new RetryableLogObserver(
            "test",
            numRetries,
            withStrategy(exception::set, TerminationStrategy.STOP_PROCESSING),
            true,
            new LogObserver() {

              @Override
              public boolean onError(Throwable error) {
                // Retryable.
                return true;
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                return true;
              }
            });

    // Retry for max-number of times.
    for (int i = 0; i < numRetries; i++) {
      Assert.assertTrue(observer.onError(new OutOfMemoryError("Test.")));
    }

    // Out of retries.
    Assert.assertFalse(observer.onError(new OutOfMemoryError("Test.")));

    // Failure counter restarts after element is successfully processed.
    observer.onNext(null, null);

    // Retry for max-number of times.
    for (int i = 0; i < numRetries; i++) {
      Assert.assertTrue(observer.onError(new OutOfMemoryError("Test.")));
    }

    // Out of retries.
    Assert.assertFalse(observer.onError(new OutOfMemoryError("Test.")));
  }

  @Test
  public void testHandleThrowableWithStrategy() {
    try {
      RetryableLogObserver.handleThrowableWithStrategy(
          null, new OutOfMemoryError(), TerminationStrategy.RETHROW);
      Assert.fail("Should have thrown exception");
    } catch (OutOfMemoryError ex) {
      // pass
    }
    try {
      RetryableLogObserver.handleThrowableWithStrategy(
          null, new IllegalArgumentException(), TerminationStrategy.RETHROW);
      Assert.fail("Should have thrown exception");
    } catch (IllegalArgumentException ex) {
      // pass
    }
    try {
      RetryableLogObserver.handleThrowableWithStrategy(
          null, new IOException(), TerminationStrategy.RETHROW);
      Assert.fail("Should have thrown exception");
    } catch (IllegalStateException ex) {
      Assert.assertTrue(ex.getCause() instanceof IOException);
    }
    Assert.assertFalse(
        RetryableLogObserver.handleThrowableWithStrategy(
            null, new OutOfMemoryError(), TerminationStrategy.STOP_PROCESSING));
  }

  private UnaryFunction<Throwable, TerminationStrategy> withStrategy(
      Consumer<Throwable> consumer, TerminationStrategy strategy) {

    return t -> {
      consumer.accept(t);
      return strategy;
    };
  }
}
