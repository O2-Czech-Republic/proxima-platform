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
package cz.o2.proxima.direct.gcloud.storage;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;

public class GCloudStorageAccessorTest {

  private final Repository repo =
      Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testNamingConventionWithBucket() {
    GCloudStorageAccessor accessor =
        new GCloudStorageAccessor(entity, URI.create("gs://bucket/path"), Collections.emptyMap());
    NamingConvention convention = accessor.getNamingConvention();
    assertTrue(convention.nameOf(1500000000000L).startsWith("/2017/07/"));
  }

  @Test
  public void testNamingConventionWithBucketAndNoPath() {
    GCloudStorageAccessor accessor =
        new GCloudStorageAccessor(entity, URI.create("gs://bucket"), Collections.emptyMap());
    NamingConvention convention = accessor.getNamingConvention();
    assertTrue(convention.nameOf(1500000000000L).startsWith("/2017/07/"));
  }

  @Test
  public void testWriterAsFactorySerializable() throws IOException, ClassNotFoundException {
    GCloudStorageAccessor accessor =
        new GCloudStorageAccessor(entity, URI.create("gs://bucket"), Collections.emptyMap());
    BulkGCloudStorageWriter writer = new BulkGCloudStorageWriter(accessor, direct.getContext());
    byte[] bytes = TestUtils.serializeObject(writer.asFactory());
    AttributeWriterBase.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(writer.getUri(), factory.apply(repo).getUri());
  }

  @Test
  public void testReaderAsFactorySerializable() throws IOException, ClassNotFoundException {
    GCloudStorageAccessor accessor =
        new GCloudStorageAccessor(entity, URI.create("gs://bucket"), Collections.emptyMap());
    GCloudLogReader reader = new GCloudLogReader(accessor, direct.getContext());
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    BatchLogReader.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(accessor.getUri(), ((GCloudLogReader) factory.apply(repo)).getAccessor().getUri());
  }
}
