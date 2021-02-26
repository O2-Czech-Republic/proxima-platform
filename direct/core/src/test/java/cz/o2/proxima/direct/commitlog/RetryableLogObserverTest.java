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

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class RetryableLogObserverTest {

  @Test
  public void testRetryableError() {
    final int numRetries = 10;
    final CommitLogReader reader = Mockito.mock(CommitLogReader.class);
    final RetryableLogObserver observer =
        RetryableLogObserver.online(
            numRetries,
            "test",
            reader,
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
      Assert.assertTrue(observer.onError(new Exception("Test.")));
    }

    // Out of retries.
    Assert.assertFalse(observer.onError(new Exception("Test.")));

    // Failure counter restarts after element is successfully processed.
    observer.onNext(null, null);

    // Retry for max-number of times.
    for (int i = 0; i < numRetries; i++) {
      Assert.assertTrue(observer.onError(new Exception("Test.")));
    }

    // Out of retries.
    Assert.assertFalse(observer.onError(new Exception("Test.")));
  }

  @Test
  public void testNonRetryableError() {
    final int numRetries = 10;
    final CommitLogReader reader = Mockito.mock(CommitLogReader.class);
    final RetryableLogObserver observer =
        RetryableLogObserver.online(
            numRetries,
            "test",
            reader,
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
    Assert.assertFalse(observer.onError(new Exception("Test.")));
  }

  @Test
  public void testStartOnline() {
    final int numRetries = 10;
    final CommitLogReader reader = Mockito.mock(CommitLogReader.class);
    final RetryableLogObserver observer =
        RetryableLogObserver.online(
            numRetries, "test", reader, (LogObserver) (ingest, context) -> true);
    observer.start();
    Mockito.verify(reader, Mockito.times(1))
        .observe(Mockito.eq("test"), Mockito.eq(Position.NEWEST), Mockito.eq(observer));
  }

  @Test
  public void testStartBulk() {
    final int numRetries = 10;
    final CommitLogReader reader = Mockito.mock(CommitLogReader.class);
    final RetryableLogObserver observer =
        RetryableLogObserver.bulk(
            numRetries, "test", reader, (LogObserver) (ingest, context) -> true);
    observer.start();
    Mockito.verify(reader, Mockito.times(1))
        .observeBulk(Mockito.eq("test"), Mockito.eq(Position.NEWEST), Mockito.eq(observer));
  }
}
