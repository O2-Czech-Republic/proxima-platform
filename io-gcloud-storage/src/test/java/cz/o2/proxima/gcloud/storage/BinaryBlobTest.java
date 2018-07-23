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

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test suite for {@link BinaryBlob} for reading and writing.
 */
@RunWith(Parameterized.class)
public class BinaryBlobTest {

  final Repository repo = Repository.of(ConfigFactory.load().resolve());
  final AttributeDescriptor<?> attr;
  final AttributeDescriptor<?> wildcard;
  final EntityDescriptor entity;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Parameterized.Parameter
  public boolean gzip;

  @Parameterized.Parameters
  public static Collection<Boolean> params() {
    return Arrays.asList(true, false);
  }

  File file;
  BinaryBlob blob;

  public BinaryBlobTest() throws URISyntaxException {
    this.wildcard = AttributeDescriptor.newBuilder(repo)
        .setEntity("dummy")
        .setSchemeUri(new URI("bytes:///"))
        .setName("wildcard.*")
        .build();
    this.attr = AttributeDescriptor.newBuilder(repo)
        .setEntity("dummy")
        .setSchemeUri(new URI("bytes:///"))
        .setName("attr")
        .build();
    this.entity = EntityDescriptor.newBuilder()
        .setName("dummy")
        .addAttribute((AttributeDescriptorBase<?>) attr)
        .addAttribute((AttributeDescriptorBase<?>) wildcard)
        .build();
  }


  @Before
  public void setUp() throws IOException {
    file = folder.newFile();
    blob = new BinaryBlob(file);
  }

  @Test
  public void testWriteAndRead() throws IOException {
    StreamElement el = StreamElement.update(
        entity, attr, UUID.randomUUID().toString(),
        "key", "attr", System.currentTimeMillis(), new byte[] { 1, 2 });

    try (BinaryBlob.Writer writer = blob.writer(gzip)) {
      writer.write(el);
    }

    try (BinaryBlob.Reader reader = blob.reader(entity)) {
      int matched = 0;
      for (StreamElement e : reader) {
        assertEquals(e.toString(), el.toString());
        matched++;
      }
      assertEquals(1, matched);
    }

  }

}
