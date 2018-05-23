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
package cz.o2.proxima.storage.http;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorImpl;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.seznam.euphoria.shadow.com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import org.junit.Test;

/**
 * Verify that all accessors are serializable.
 */
public class SerializableTest {

  Repository repo = Repository.of(ConfigFactory.load());
  AttributeDescriptor<byte[]> attr = AttributeDescriptor.newBuilder(repo)
      .setName("attr")
      .setEntity("entity")
      .setSchemeURI(new URI("bytes:///"))
      .build();
  EntityDescriptor entity = EntityDescriptor.newBuilder()
      .setName("entity")
      .addAttribute((AttributeDescriptorImpl) attr)
      .build();

  public SerializableTest() throws Exception {

  }

  @Test
  public void testHttpWriter() throws Exception {
    HttpWriter writer = new HttpWriter(
        entity, new URI("http://test/"), Collections.emptyMap());
    checkSerializable(writer);
  }

  @Test
  public void testWebsocketReader() throws Exception {
    WebsocketReader reader = new WebsocketReader(
        entity, new URI("ws://test"), new HashMap<String, Object>() {{
          put("hello", "hi");
          put("attributes", Lists.newArrayList("*"));
        }});
    checkSerializable(reader);
  }

  private void checkSerializable(Object o) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(o);
    }
  }

}
