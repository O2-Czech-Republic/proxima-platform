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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;

public class LogObserversTest {

  @Test
  public void testSynchronizedLogObserver() {
    LogObserver delegate = mock(LogObserver.class);
    LogObserver observer = LogObservers.synchronizedObserver(delegate);
    Throwable err = new Throwable();
    observer.onError(err);
    verify(delegate).onError(err);
    observer.onCompleted();
    verify(delegate).onCompleted();
    observer.onIdle(null);
    verify(delegate).onIdle(null);
    observer.onNext(null, null);
    verify(delegate).onNext(null, null);
    Error error = new Error();
    observer.onFatalError(error);
    verify(delegate).onFatalError(error);
    observer.onRepartition(null);
    verify(delegate).onRepartition(null);
    observer.onCancelled();
    verify(delegate).onCancelled();
  }
}
