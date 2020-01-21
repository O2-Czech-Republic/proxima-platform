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
package cz.o2.proxima.beam.core.io;

import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import org.junit.Test;

/** Test suite for {@link StreamElementCoder}. */
public class StreamElementCoderTest {

  @Test
  public void testCoderSerializable() throws IOException, ClassNotFoundException {
    StreamElementCoder coder =
        StreamElementCoder.of(Repository.of(() -> ConfigFactory.load("test-reference.conf")));
    final byte[] serialized = TestUtils.serializeObject(coder);
    assertTrue(
        "Length of serialized bytes should be less than 2 KiB, got " + serialized.length,
        serialized.length < 2048);
    TestUtils.assertSerializable(coder);
  }
}
