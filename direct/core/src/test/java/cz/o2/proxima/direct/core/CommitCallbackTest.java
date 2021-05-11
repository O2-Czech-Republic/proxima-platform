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
package cz.o2.proxima.direct.core;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class CommitCallbackTest {

  @Test
  public void testCommitAfterNumCommits() {
    AtomicBoolean success = new AtomicBoolean();
    AtomicReference<Throwable> error = new AtomicReference<>();
    CommitCallback commit =
        CommitCallback.afterNumCommits(
            2,
            (succ, exc) -> {
              success.set(succ);
              error.set(exc);
            });
    assertFalse(success.get());
    assertNull(error.get());
    commit.commit(true, null);
    assertFalse(success.get());
    assertNull(error.get());
    commit.commit(true, null);
    assertTrue(success.get());
    assertNull(error.get());
    commit.commit(false, new Throwable());
    assertTrue(success.get());
    assertNull(error.get());
    commit =
        CommitCallback.afterNumCommits(
            2,
            (succ, exc) -> {
              success.set(succ);
              error.set(exc);
            });
    commit.commit(false, new Throwable());
    assertFalse(success.get());
    assertNotNull(error.get());
    error.set(null);
    commit.commit(false, new Throwable());
    assertNull(error.get());
  }
}
