/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.s3;

import static cz.o2.proxima.direct.io.s3.S3FileSystemTest.cfg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.io.bulkfs.NamingConvention;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.net.URI;
import org.junit.Test;

public class S3AccessorTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testCreateAccessorFromDescriptor() {
    S3StorageDescriptor descriptor = new S3StorageDescriptor();
    assertNotNull(
        descriptor.createAccessor(
            direct, TestUtils.createTestFamily(entity, URI.create("s3://bucket/path"))));
  }

  @Test
  public void testNamingConventionWithBucket() {
    S3Accessor accessor =
        new S3Accessor(TestUtils.createTestFamily(entity, URI.create("s3://bucket/path")));
    NamingConvention convention = accessor.getNamingConvention();
    assertTrue(convention.nameOf(1500000000000L).startsWith("/2017/07/"));
  }

  @Test
  public void testNamingConventionWithBucketAndNoPath() {
    S3Accessor accessor =
        new S3Accessor(TestUtils.createTestFamily(entity, URI.create("s3://bucket/path")));
    NamingConvention convention = accessor.getNamingConvention();
    assertTrue(convention.nameOf(1500000000000L).startsWith("/2017/07/"));
  }

  @Test
  public void testWriterAsFactorySerializable() throws IOException, ClassNotFoundException {
    S3Accessor accessor =
        new S3Accessor(TestUtils.createTestFamily(entity, URI.create("s3://bucket/path"), cfg()));
    BulkS3Writer writer = new BulkS3Writer(accessor, direct.getContext());
    byte[] bytes = TestUtils.serializeObject(writer.asFactory());
    AttributeWriterBase.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(writer.getUri(), factory.apply(repo).getUri());
  }

  @Test
  public void testReaderAsFactorySerializable() throws IOException, ClassNotFoundException {
    S3Accessor accessor =
        new S3Accessor(TestUtils.createTestFamily(entity, URI.create("s3://bucket/path"), cfg()));
    S3LogReader reader = new S3LogReader(accessor, direct.getContext());
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    BatchLogReader.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(accessor.getUri(), ((S3LogReader) factory.apply(repo)).getAccessor().getUri());
  }
}
