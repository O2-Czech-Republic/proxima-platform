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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/** Test {@link SerializableScopedValue}. */
public class SerializableScopedValueTest {

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    SerializableScopedValue<Integer, Integer> value = new SerializableScopedValue<>(() -> 1);
    SerializableScopedValue<Integer, Integer> other = TestUtils.assertSerializable(value);
    TestUtils.assertHashCodeAndEquals(value, other);
  }

  @Test
  public void testContextLocality() throws IOException, ClassNotFoundException {
    BlockingQueue<Integer> results = new LinkedBlockingDeque<>();
    SerializableScopedValue<Integer, AtomicInteger> value =
        new SerializableScopedValue<>(() -> new AtomicInteger(1));
    value.reset(1);
    SerializableScopedValue<Integer, AtomicInteger> other =
        TestUtils.deserializeObject(TestUtils.serializeObject(value));
    assertEquals(1, other.get(1).getAndAdd(1));
    assertEquals(1, other.get(2).getAndAdd(2));
    assertEquals(2, other.get(1).get());
    assertEquals(3, other.get(2).get());
    other.reset(1);
    assertEquals(1, other.get(1).get());
  }
}
