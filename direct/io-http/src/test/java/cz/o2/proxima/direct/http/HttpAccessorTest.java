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
package cz.o2.proxima.direct.http;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;

/** Test {@link HttpAccessor}. */
public class HttpAccessorTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor entity = repo.getEntity("gateway");

  @Test
  public void testWriterAsFactorySerializable() throws IOException, ClassNotFoundException {
    HttpAccessor accessor =
        new HttpAccessor(entity, URI.create("https://host"), Collections.emptyMap());
    HttpWriter writer = new HttpWriter(entity, accessor.getUri(), accessor.cfg);
    byte[] bytes = TestUtils.serializeObject(writer.asFactory());
    AttributeWriterBase.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(writer.getUri(), factory.apply(repo).getUri());
  }

  @Test
  public void testReaderAsFactorySerializable() throws IOException, ClassNotFoundException {
    HttpAccessor accessor =
        new HttpAccessor(
            entity,
            URI.create("https://host"),
            ImmutableMap.<String, Object>builder()
                .put("attributes", Collections.singletonList("armed"))
                .put("hello", "")
                .build());
    WebsocketReader reader = new WebsocketReader(entity, accessor.getUri(), accessor.cfg);
    byte[] bytes = TestUtils.serializeObject(reader.asFactory());
    CommitLogReader.Factory<?> factory = TestUtils.deserializeObject(bytes);
    assertEquals(reader.getUri(), ((WebsocketReader) factory.apply(repo)).getUri());
  }
}
