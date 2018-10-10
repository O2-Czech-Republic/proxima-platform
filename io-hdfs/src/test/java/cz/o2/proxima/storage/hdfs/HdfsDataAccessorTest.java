/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage.hdfs;

import com.google.common.collect.Maps;
import cz.o2.proxima.repository.EntityDescriptor;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.URI;

import static org.junit.Assert.assertTrue;

public class HdfsDataAccessorTest {

  @Test
  public void testSerializable() throws Exception {
    HdfsDataAccessor writer = new HdfsDataAccessor(
        EntityDescriptor.newBuilder().setName("dummy").build(),
        new URI("file://dummy/dir"), Maps.newHashMap());

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(buffer)) {
      oos.writeObject(writer);
    }
    assertTrue(buffer.toByteArray().length > 0);
  }
}
