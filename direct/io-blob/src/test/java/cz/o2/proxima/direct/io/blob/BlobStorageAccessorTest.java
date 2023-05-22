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
package cz.o2.proxima.direct.io.blob;

import static org.junit.Assert.assertTrue;

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.io.blob.TestBlobStorageAccessor.TestBlob;
import cz.o2.proxima.direct.io.blob.TestBlobStorageAccessor.TestBlobPath;
import cz.o2.proxima.direct.io.bulkfs.FileSystem;
import cz.o2.proxima.direct.io.bulkfs.NamingConvention;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import org.junit.Test;

public class BlobStorageAccessorTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testNamingConventionWithBucket() {
    BlobStorageAccessor accessor =
        new TestBlobStorageAccessor(
            TestUtils.createTestFamily(entity, URI.create("blob-test://bucket/path")));
    NamingConvention convention = accessor.getNamingConvention();
    assertTrue(convention.nameOf(1500000000000L).startsWith("/2017/07/"));
  }

  @Test
  public void testNamingConventionWithBucketAndNoPath() {
    BlobStorageAccessor accessor =
        new TestBlobStorageAccessor(
            TestUtils.createTestFamily(entity, URI.create("blob-test://bucket")));
    NamingConvention convention = accessor.getNamingConvention();
    assertTrue(convention.nameOf(1500000000000L).startsWith("/2017/07/"));
  }

  @Test
  public void testBlobPathSerializable() throws IOException, ClassNotFoundException {
    TestBlobStorageAccessor accessor =
        new TestBlobStorageAccessor(
            TestUtils.createTestFamily(entity, URI.create("blob-test://bucket")));

    FileSystem fs = accessor.new TestBlobFileSystem();
    TestBlob blob = accessor.new TestBlob("test");
    TestBlobPath path = accessor.new TestBlobPath(fs, blob);
    TestBlobPath path2 = TestUtils.assertSerializable(path);
    TestUtils.assertHashCodeAndEquals(path, path2);
    TestUtils.assertHashCodeAndEquals(fs, path.getFileSystem());
  }
}
