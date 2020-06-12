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
import org.junit.Test;

public class S3BlobPathTest implements Serializable {

  Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  DirectDataOperator op = repo.getOrCreateOperator(DirectDataOperator.class);
  EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    Context context = op.getContext();
    S3FileSystem fs =
        new S3FileSystem(
            new S3Accessor(entity, URI.create("gs://bucket"), S3FileSystemTest.cfg()), context);
    S3BlobPath path = new S3BlobPath(context, fs, new S3Blob("name"));
    S3BlobPath path2 = TestUtils.assertSerializable(path);
    TestUtils.assertHashCodeAndEquals(path, path2);
  }
}
