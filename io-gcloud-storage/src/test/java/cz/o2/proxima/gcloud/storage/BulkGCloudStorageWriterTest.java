/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.gcloud.storage;

import com.google.cloud.storage.Blob;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import static org.mockito.Mockito.*;

/**
 * Test suite for {@link BulkGCloudStorageWriter}.
 */
@RunWith(Parameterized.class)
public class BulkGCloudStorageWriterTest {

  final Repository repo = Repository.of(ConfigFactory.load().resolve());
  final AttributeDescriptor<?> attr;
  final AttributeDescriptor<?> wildcard;
  final EntityDescriptor entity;

  @Parameterized.Parameters
  public static Collection<Boolean> parameters() {
    return Arrays.asList(true, false);
  }

  @Parameterized.Parameter
  public boolean gzip;

  BulkGCloudStorageWriter writer;
  List<String> blobs;
  List<StreamElement> written;
  AtomicReference<CountDownLatch> latch = new AtomicReference<>();

  public BulkGCloudStorageWriterTest() throws URISyntaxException {
    this.wildcard = AttributeDescriptor.newBuilder(repo)
        .setEntity("dummy")
        .setSchemeURI(new URI("bytes:///"))
        .setName("wildcard.*")
        .build();
    this.attr = AttributeDescriptor.newBuilder(repo)
        .setEntity("dummy")
        .setSchemeURI(new URI("bytes:///"))
        .setName("attr")
        .build();
    this.entity = EntityDescriptor.newBuilder()
        .setName("dummy")
        .addAttribute((AttributeDescriptorBase<?>) attr)
        .addAttribute((AttributeDescriptorBase<?>) wildcard)
        .build();
  }

  @Before
  public void setUp() throws URISyntaxException {
    blobs = Collections.synchronizedList(new ArrayList<>());
    written = null;
    writer = new BulkGCloudStorageWriter(
        entity, new URI("gcloud-storage://project:bucket/path"),
        cfg(), context()) {

      @Override
      Blob createBlob(String name) {
        blobs.add(name);
        return mock(Blob.class);
      }

      @Override
      String uuid() {
        return "uuid";
      }

      @Override
      void flushToBlob(File file, Blob blob) throws IOException {
        written = new ArrayList<>();
        try (BinaryBlob.Reader reader = new BinaryBlob(file).reader(entity)) {
          reader.iterator().forEachRemaining(written::add);
        }
        if (latch.get() != null) {
          latch.get().countDown();
        }
      }

    };
  }

  @Test(timeout = 2000)
  public void testWrite() throws Exception {
    latch.set(new CountDownLatch(2));
    long now = 1500000000000L;
    StreamElement first = StreamElement.update(entity, attr,
        UUID.randomUUID().toString(),
        "key", "attr", now, new byte[] { 1, 2 });
    StreamElement second = StreamElement.update(entity, wildcard,
        UUID.randomUUID().toString(),
        "key", "wildcard.1", now + 2000, new byte[] { 3 });
    writer.write(first, (succ, exc) -> {
      // this one will not get committed
    });
    // this will flush on next write
    writer.flush();
    writer.write(second, (succ, exc) -> {
      assertTrue("Exception " + exc, succ);
      assertNull(exc);
      latch.get().countDown();
    });
    latch.get().await();
    assertNotNull(written);
    validate(written, first, second);
    assertEquals(1, blobs.size());
    assertEquals(
        writer.toBlobName(now - now % 1000, now + 2000),
        blobs.get(0));
    assertTrue(blobs.get(0).startsWith("2017/07/"));
  }

  private void validate(List<StreamElement> written, StreamElement... elements)
      throws IOException {

    Iterator<StreamElement> iterator = written.iterator();
    assertEquals(
        "Expected " + Arrays.toString(elements) + " got " + written,
        elements.length, written.size());
    for (StreamElement el : elements) {
      StreamElement next = iterator.next();
      assertEquals(next, el);
    }
  }

  private Map<String, Object> cfg() {
    Map<String, Object> ret = new HashMap<>();
    ret.put("log-roll-interval", "1000");
    if (gzip) {
      ret.put("gzip", "true");
    }
    return ret;
  }

  private static Context context() {
    return new Context(() -> Executors.newCachedThreadPool()) { };
  }

}
