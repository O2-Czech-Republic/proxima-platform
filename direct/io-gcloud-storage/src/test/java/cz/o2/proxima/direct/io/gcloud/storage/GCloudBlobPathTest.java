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
package cz.o2.proxima.direct.io.gcloud.storage;

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import org.junit.Test;

public class GCloudBlobPathTest implements Serializable {

  Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    GCloudFileSystem fs =
        new GCloudFileSystem(
            new GCloudStorageAccessor(
                TestUtils.createTestFamily(entity, URI.create("gs://bucket"))));
    GCloudBlobPath path =
        new GCloudBlobPath(fs, null) {
          @Override
          public String getBlobName() {
            return "name";
          }
        };
    GCloudBlobPath path2 = TestUtils.assertSerializable(path);
    TestUtils.assertHashCodeAndEquals(path, path2);
  }
}
