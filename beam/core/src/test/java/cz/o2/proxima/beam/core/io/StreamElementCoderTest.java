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
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.TestUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.coders.Coder;
import org.junit.Test;

/** Test suite for {@link StreamElementCoder}. */
public class StreamElementCoderTest {

  private final Repository repo = Repository.of(ConfigFactory.load("test-reference.conf"));
  private final Coder<StreamElement> coder = StreamElementCoder.of(repo);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<Object> armed = gateway.getAttribute("armed");
  private final AttributeDescriptor<Object> device = gateway.getAttribute("device.*");

  @Test
  public void testCoderSerializable() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(coder);
  }

  @Test
  public void testStreamElement() throws IOException {
    List<StreamElement> elements =
        Arrays.asList(
            StreamElement.upsert(
                gateway,
                armed,
                UUID.randomUUID().toString(),
                "key",
                armed.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            StreamElement.delete(
                gateway,
                armed,
                UUID.randomUUID().toString(),
                "key",
                armed.getName(),
                System.currentTimeMillis()),
            StreamElement.deleteWildcard(
                gateway, device, UUID.randomUUID().toString(), "key", System.currentTimeMillis()));

    elements.forEach(
        e -> assertEquals(e, ExceptionUtils.uncheckedFactory(() -> decode(encode(e)))));
  }

  @Test
  public void testStreamElementWithSequentialId() throws IOException {
    List<StreamElement> elements =
        Arrays.asList(
            StreamElement.upsert(
                gateway,
                armed,
                1L,
                "key",
                armed.getName(),
                System.currentTimeMillis(),
                new byte[] {1, 2, 3}),
            StreamElement.delete(
                gateway, armed, 2L, "key", armed.getName(), System.currentTimeMillis()));

    elements.forEach(
        e -> assertEquals(e, ExceptionUtils.uncheckedFactory(() -> decode(encode(e)))));
  }

  @Test
  public void testCoderOutputReasonableSize() throws IOException {
    StreamElement element =
        StreamElement.upsert(
            gateway,
            armed,
            UUID.randomUUID().toString(),
            "key",
            armed.getName(),
            System.currentTimeMillis(),
            new byte[] {1, 2, 3});
    assertTrue(encode(element).length < 100);
  }

  private byte[] encode(StreamElement element) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    coder.encode(element, baos);
    return baos.toByteArray();
  }

  private StreamElement decode(byte[] bytes) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    return coder.decode(bais);
  }
}
