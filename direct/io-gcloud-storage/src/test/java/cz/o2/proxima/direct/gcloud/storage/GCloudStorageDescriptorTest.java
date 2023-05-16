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
package cz.o2.proxima.direct.gcloud.storage;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.storage.internal.AbstractDataAccessorFactory.Accept;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import java.io.IOException;
import java.net.URI;
import org.junit.Test;

public class GCloudStorageDescriptorTest {

  private final DataAccessorFactory factory = new GCloudStorageDescriptor();

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    TestUtils.assertSerializable(factory);
  }

  @Test
  public void testHashCodeAndEquals() {
    TestUtils.assertHashCodeAndEquals(factory, new GCloudStorageDescriptor());
  }

  @Test
  public void testAcceptSchema() {
    assertEquals(Accept.ACCEPT, factory.accepts(URI.create("gs://bucket/path")));
    assertEquals(Accept.REJECT, factory.accepts(URI.create("file:///")));
  }
}
