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

import static org.junit.Assert.*;

import com.google.common.collect.Streams;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test {@link JsonFormat}. */
@RunWith(Parameterized.class)
public class JsonFormatTest {

  @Parameterized.Parameters
  public static Collection<Boolean> parameters() {
    return Arrays.asList(true, false);
  }

  @Parameterized.Parameter public boolean gzip;

  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final Repository repo =
      Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor entity = repo.getEntity("gateway");
  private final AttributeDescriptor<Object> wildcard = entity.getAttribute("device.*");
  private final long stamp = System.currentTimeMillis();

  private JsonFormat format;

  @Before
  public void setUp() {
    this.format = new JsonFormat(gzip);
  }

  @Test
  public void testReadWrite() throws IOException {
    folder.create();
    File file = folder.newFile();
    FileSystem fs =
        FileSystem.local(
            file.getParentFile(),
            NamingConvention.defaultConvention(Duration.ofHours(1), "prefix", format.fileSuffix()));
    AttributeDescriptor<Object> wildcard = entity.getAttribute("device.*");
    StreamElement deleteWildcard = deleteWildcard();
    StreamElement delete = delete();
    StreamElement upsert = upsert();
    try (Writer writer = format.openWriter(Path.local(fs, file), entity)) {
      writer.write(deleteWildcard);
      writer.write(delete);
      writer.write(upsert);
    }
    try (Reader reader = format.openReader(Path.local(fs, file), entity)) {
      List<StreamElement> elements = Streams.stream(reader).collect(Collectors.toList());
      assertEquals(Arrays.asList(deleteWildcard, delete, upsert), elements);
    }
  }

  @Test
  public void testToAndFromStreamElement() {
    checkSerialization(deleteWildcard());
    checkSerialization(delete());
    checkSerialization(upsert());
  }

  StreamElement upsert() {
    return StreamElement.upsert(
        entity,
        wildcard,
        UUID.randomUUID().toString(),
        "key",
        wildcard.toAttributePrefix() + "1",
        stamp,
        new byte[] {1});
  }

  StreamElement delete() {
    return StreamElement.delete(
        entity,
        wildcard,
        UUID.randomUUID().toString(),
        "key",
        wildcard.toAttributePrefix() + "1",
        stamp);
  }

  StreamElement deleteWildcard() {
    return StreamElement.deleteWildcard(
        entity, wildcard, UUID.randomUUID().toString(), "key", stamp);
  }

  private void checkSerialization(StreamElement elem) {
    assertEquals(elem, JsonFormat.toStreamElement(JsonFormat.toJsonElement(elem), entity));
  }
}
