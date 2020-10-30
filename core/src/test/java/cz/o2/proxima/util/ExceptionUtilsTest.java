/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Test {@link ExceptionUtils}. */
public class ExceptionUtilsTest {

  @Test
  public void testIgnoringInterrupted() {
    assertTrue(
        ExceptionUtils.ignoringInterrupted(
            () -> {
              throw new InterruptedException();
            }));
    assertFalse(ExceptionUtils.ignoringInterrupted(() -> {}));
  }

  @Test
  public void testIsInterrupted() {
    assertFalse(ExceptionUtils.isInterrupted(new RuntimeException()));
    assertTrue(ExceptionUtils.isInterrupted(new InterruptedException()));
    assertTrue(ExceptionUtils.isInterrupted(new RuntimeException(new InterruptedException())));
  }
}
