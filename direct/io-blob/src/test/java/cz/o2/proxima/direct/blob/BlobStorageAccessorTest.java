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
package cz.o2.proxima.direct.blob;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.blob.TestBlobStorageAccessor.TestBlob;
import cz.o2.proxima.direct.blob.TestBlobStorageAccessor.TestBlobPath;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;

public class BlobStorageAccessorTest {

  private final Repository repo =
      Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testNamingConventionWithBucket() {
    BlobStorageAccessor accessor =
        new TestBlobStorageAccessor(
            entity, URI.create("blob-test://bucket/path"), Collections.emptyMap());
    NamingConvention convention = accessor.getNamingConvention();
    assertTrue(convention.nameOf(1500000000000L).startsWith("/2017/07/"));
  }

  @Test
  public void testNamingConventionWithBucketAndNoPath() {
    BlobStorageAccessor accessor =
        new TestBlobStorageAccessor(
            entity, URI.create("blob-test://bucket"), Collections.emptyMap());
    NamingConvention convention = accessor.getNamingConvention();
    assertTrue(convention.nameOf(1500000000000L).startsWith("/2017/07/"));
  }

  @Test
  public void testBlobPathSerializable() throws IOException, ClassNotFoundException {
    TestBlobStorageAccessor accessor =
        new TestBlobStorageAccessor(
            entity, URI.create("blob-test://bucket"), Collections.emptyMap());

    FileSystem fs = accessor.new TestBlobFileSystem();
    TestBlob blob = accessor.new TestBlob("test");
    TestBlobPath path = accessor.new TestBlobPath(fs, blob);
    TestBlobPath path2 = TestUtils.assertSerializable(path);
    TestUtils.assertHashCodeAndEquals(path, path2);
    TestUtils.assertHashCodeAndEquals(fs, path.getFileSystem());
  }
}
