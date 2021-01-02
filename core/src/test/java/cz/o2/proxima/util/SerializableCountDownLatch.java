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
package cz.o2.proxima.util;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;

/**
 * This is pseudo serializable {@code CountDownLatch}. This is just for testing consumption in
 * LocalExecutor
 */
public class SerializableCountDownLatch implements Serializable {

  // we don't actually serialize this
  private transient CountDownLatch latch;

  public SerializableCountDownLatch(int value) {
    this.latch = new CountDownLatch(value);
  }

  public void countDown() {
    latch.countDown();
  }

  public void await() throws InterruptedException {
    latch.await();
  }
}
