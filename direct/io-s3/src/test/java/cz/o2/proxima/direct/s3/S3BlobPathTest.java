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
package cz.o2.proxima.direct.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.s3.S3BlobPath.S3Blob;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class S3BlobPathTest implements Serializable {

  Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  DirectDataOperator op = repo.getOrCreateOperator(DirectDataOperator.class);
  EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testBlobGetSize() {
    final S3FileSystem fs = Mockito.mock(S3FileSystem.class);
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(1337L);
    final S3Object object = new S3Object();
    object.setObjectMetadata(objectMetadata);
    Mockito.when(fs.getObject(Mockito.eq("name"))).thenReturn(object);
    final S3Blob blob = new S3Blob("name", fs);
    Assert.assertEquals(1337L, blob.getSize());
  }

  @Test
  public void testBlobGetSizeOnException() {
    final S3FileSystem fs = Mockito.mock(S3FileSystem.class, Mockito.RETURNS_DEEP_STUBS);
    Mockito.when(fs.getObject(Mockito.any())).thenThrow(new IllegalStateException());
    final S3Blob blob = new S3Blob("name", fs);
    Assert.assertEquals(0L, blob.getSize());
  }

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    Context context = op.getContext();
    S3FileSystem fs =
        new S3FileSystem(
            new S3Accessor(entity, URI.create("gs://bucket"), S3FileSystemTest.cfg()), context);
    S3BlobPath path = new S3BlobPath(context, fs, new S3Blob("name", fs));
    S3BlobPath path2 = TestUtils.assertSerializable(path);
    TestUtils.assertHashCodeAndEquals(path, path2);
  }
}
