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
package cz.o2.proxima.direct.bulk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.bulk.BinaryBlobFormat.BinaryBlobReader;
import cz.o2.proxima.direct.bulk.BinaryBlobFormat.BinaryBlobWriter;
import cz.o2.proxima.gcloud.storage.proto.Serialization.Header;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test suite for {@link BinaryBlobFormat} for reading and writing. */
@RunWith(Parameterized.class)
public class BinaryBlobFormatTest {

  final Repository repo = Repository.of(ConfigFactory.load().resolve());
  final AttributeDescriptor<?> attr;
  final AttributeDescriptor<?> wildcard;
  final EntityDescriptor entity;

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  @Parameterized.Parameter public boolean gzip;

  @Parameterized.Parameters
  public static Collection<Boolean> params() {
    return Arrays.asList(true, false);
  }

  Path file;
  BinaryBlobFormat blob;

  public BinaryBlobFormatTest() throws URISyntaxException {
    this.wildcard =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setSchemeUri(new URI("bytes:///"))
            .setName("wildcard.*")
            .build();
    this.attr =
        AttributeDescriptor.newBuilder(repo)
            .setEntity("dummy")
            .setSchemeUri(new URI("bytes:///"))
            .setName("attr")
            .build();
    this.entity =
        EntityDescriptor.newBuilder()
            .setName("dummy")
            .addAttribute(attr)
            .addAttribute(wildcard)
            .build();
  }

  @Before
  public void setUp() throws IOException {
    blob = new BinaryBlobFormat(gzip);
    File file = folder.newFile();
    FileSystem fs =
        FileSystem.local(
            file.getParentFile(),
            NamingConvention.defaultConvention(Duration.ofHours(1), "prefix", blob.fileSuffix()));
    this.file = Path.local(fs, file);
  }

  @After
  public void tearDown() {
    ExceptionUtils.unchecked(file::delete);
  }

  @Test
  public void testWriteAndRead() throws IOException {
    testWriteAndReadWithElement(
        StreamElement.upsert(
            entity,
            attr,
            UUID.randomUUID().toString(),
            "key",
            "attr",
            System.currentTimeMillis(),
            new byte[] {1, 2}));
  }

  @Test
  public void testWriteAndReadDelete() throws IOException {
    testWriteAndReadWithElement(
        StreamElement.delete(
            entity, attr, UUID.randomUUID().toString(), "key", "attr", System.currentTimeMillis()));
  }

  @Test
  public void testWriteAndReadDeleteWildcard() throws IOException {
    testWriteAndReadWithElement(
        StreamElement.deleteWildcard(
            entity, wildcard, UUID.randomUUID().toString(), "key", System.currentTimeMillis()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReadInvalidMagic() throws IOException {
    byte[] bytes = Header.newBuilder().setMagic("INVALID").build().toByteArray();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(bytes.length);
    dos.write(bytes);
    dos.flush();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    List<StreamElement> elements =
        Lists.newArrayList(new BinaryBlobFormat.BinaryBlobReader(file, entity, bais).iterator());
    assertTrue(elements.isEmpty());
  }

  @Test
  public void testEmptyStream() throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(new byte[] {});
    List<StreamElement> elements =
        Lists.newArrayList(new BinaryBlobFormat.BinaryBlobReader(file, entity, bais).iterator());
    assertTrue(elements.isEmpty());
  }

  void testWriteAndReadWithElement(StreamElement el) throws IOException {
    try (BinaryBlobWriter writer = blob.openWriter(file, entity)) {
      writer.write(el);
    }

    try (BinaryBlobReader reader = blob.openReader(file, entity)) {
      int matched = 0;
      for (StreamElement e : reader) {
        assertEquals(e.toString(), el.toString());
        matched++;
      }
      assertEquals(1, matched);
    }
  }
}
