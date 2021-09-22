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
package cz.o2.proxima.beam.core.io;

import static org.junit.Assert.assertEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;

/** Test suite for {@link AttributeDescriptorCoder}. */
public class AttributeDescriptorCoderTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());

  @Test
  public void testSerializeDeserialize() throws IOException {
    AttributeDescriptorCoder coder = AttributeDescriptorCoder.of(repo);
    byte[] serialized;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      coder.encode(repo.getEntity("event").getAttribute("data"), baos);
      serialized = baos.toByteArray();
    }
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      AttributeDescriptor<?> desc = coder.decode(bais);
      assertEquals("data", desc.getName());
    }
  }
}
