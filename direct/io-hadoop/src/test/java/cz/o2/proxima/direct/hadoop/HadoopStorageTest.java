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
package cz.o2.proxima.direct.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.internal.AbstractDataAccessorFactory.Accept;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HadoopStorageTest {

  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testSerialize() throws IOException, ClassNotFoundException {
    HadoopStorage storage = new HadoopStorage();
    TestUtils.assertSerializable(storage);
  }

  @Test
  public void testHashCodeAndEquals() {
    TestUtils.assertHashCodeAndEquals(new HadoopStorage(), new HadoopStorage());

    EntityDescriptor entity = EntityDescriptor.newBuilder().setName("dummy").build();
    TestUtils.assertHashCodeAndEquals(
        new HadoopDataAccessor(entity, URI.create("hdfs://host:9000/path"), Collections.emptyMap()),
        new HadoopDataAccessor(entity, URI.create("hdfs://host:9000/path"), Collections.emptyMap()));
  }

  @Test
  public void testAcceptScheme() {
    HadoopStorage storage = new HadoopStorage();
    assertEquals(Accept.ACCEPT, storage.accepts(URI.create("hdfs://host:9000/path")));
    assertEquals(Accept.ACCEPT, storage.accepts(URI.create("hadoop:file:///path")));
    assertEquals(Accept.REJECT, storage.accepts(URI.create("file:///path")));
  }

  @Test(timeout = 5000L)
  public void testWriteElement() throws InterruptedException {

    Map<String, Object> cfg = new HashMap<>();
    cfg.put(HadoopDataAccessor.HDFS_MIN_ELEMENTS_TO_FLUSH, 1);
    cfg.put(HadoopDataAccessor.HDFS_ROLL_INTERVAL, -1);

    CountDownLatch latch = new CountDownLatch(1);
    writeOneElementWithConfig(
        cfg,
        ((success, error) -> {
          assertTrue(success);
          assertNull(error);
          latch.countDown();
        }));
    latch.await();
  }

  private void writeOneElementWithConfig(Map<String, Object> cfg, CommitCallback callback) {

    final Repository repository =
        ConfigRepository.Builder.ofTest(() -> ConfigFactory.defaultApplication()).build();

    EntityDescriptor entity = EntityDescriptor.newBuilder().setName("dummy").build();
    AttributeDescriptor<byte[]> attribute =
        AttributeDescriptor.newBuilder(repository)
            .setEntity("dummy")
            .setName("attribute")
            .setSchemeUri(URI.create("bytes:///"))
            .build();

    URI uri = URI.create(String.format("file://%s/dummy", tempFolder.getRoot().getAbsolutePath()));

    StreamElement element =
        StreamElement.upsert(
            entity,
            attribute,
            UUID.randomUUID().toString(),
            "test",
            attribute.getName(),
            System.currentTimeMillis(),
            "test value".getBytes());

    HadoopDataAccessor accessor = new HadoopDataAccessor(entity, uri, cfg);
    Optional<AttributeWriterBase> writer =
        accessor.newWriter(repository.getOrCreateOperator(DirectDataOperator.class).getContext());
    assertTrue(writer.isPresent());

    BulkAttributeWriter bulk = writer.get().bulk();

    bulk.write(element, element.getStamp(), callback);
  }
}
