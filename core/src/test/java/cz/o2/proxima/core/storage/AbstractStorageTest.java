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
package cz.o2.proxima.core.storage;

import static org.junit.Assert.assertThrows;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.AbstractStorage.SerializableAbstractStorage;
import cz.o2.proxima.core.util.TestUtils;
import java.io.IOException;
import java.io.NotSerializableException;
import java.net.URI;
import org.junit.Test;

public class AbstractStorageTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");

  @Test
  public void testAbstractStorageNotSerializable() {
    AbstractStorage storage = getStorage(gateway);
    assertThrows(NotSerializableException.class, () -> TestUtils.serializeObject(storage));
  }

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    AbstractStorage storage = getSerializableStorage(gateway);
    TestUtils.assertSerializable(storage);
  }

  private static SerializableAbstractStorage getSerializableStorage(EntityDescriptor entity) {
    return new SerializableAbstractStorage(entity, URI.create("inmem:///")) {};
  }

  private static AbstractStorage getStorage(EntityDescriptor entity) {
    return new AbstractStorage(entity, URI.create("inmem:///")) {};
  }
}
