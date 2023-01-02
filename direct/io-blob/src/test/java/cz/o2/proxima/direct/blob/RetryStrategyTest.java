/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.blob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cz.o2.proxima.functional.Factory;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class RetryStrategyTest {

  private static class RetryableException extends RuntimeException {
    RetryableException(String what) {
      super(what);
    }
  }

  @Test
  public void testRetry() {
    RetryStrategy retry =
        new RetryStrategy(100, 400).withRetryableException(RetryableException.class);
    AtomicInteger round = new AtomicInteger();
    AtomicInteger failUntil = new AtomicInteger(1);
    Factory<Integer> callable =
        () -> {
          if (round.incrementAndGet() <= failUntil.get()) {
            throw new RetryableException("Fail");
          }
          return round.get();
        };
    assertEquals(2, (int) retry.retry(callable));
    round.set(0);
    failUntil.set(2);
    assertEquals(3, (int) retry.retry(callable));
    failUntil.set(Integer.MAX_VALUE);
    try {
      retry.retry((Runnable) callable::apply);
      fail("Should have thrown exception");
    } catch (RetryableException ex) {
      // pass
    }
  }

  @Test(expected = RetryableException.class)
  public void testRetryNotRetryable() {
    RetryStrategy retry = new RetryStrategy(100, 400);
    AtomicInteger round = new AtomicInteger();
    AtomicInteger failUntil = new AtomicInteger(1);
    Factory<Integer> callable =
        () -> {
          if (round.incrementAndGet() <= failUntil.get()) {
            throw new RetryableException("Fail");
          }
          return round.get();
        };
    retry.retry(callable);
    fail("Should have thrown exception");
  }
}
