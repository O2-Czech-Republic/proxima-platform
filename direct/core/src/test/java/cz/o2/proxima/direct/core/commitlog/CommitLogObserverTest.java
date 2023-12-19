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
package cz.o2.proxima.direct.core.commitlog;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.storage.StreamElement;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class CommitLogObserverTest {

  @Test
  public void testOnErrorDefaultRouting() {
    final AtomicInteger numExceptions = new AtomicInteger();
    final AtomicInteger numFatalErrors = new AtomicInteger();
    final CommitLogObserver observer =
        new CommitLogObserver() {

          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            return false;
          }

          @Override
          public boolean onException(Exception exception) {
            numExceptions.incrementAndGet();
            return false;
          }

          @Override
          public boolean onFatalError(Error error) {
            numFatalErrors.incrementAndGet();
            return false;
          }
        };
    observer.onError(new Exception("Test exception"));
    assertEquals(1, numExceptions.get());
    assertEquals(0, numFatalErrors.get());
    observer.onError(new Error("Test error"));
    assertEquals(1, numExceptions.get());
    assertEquals(1, numFatalErrors.get());
  }
}
