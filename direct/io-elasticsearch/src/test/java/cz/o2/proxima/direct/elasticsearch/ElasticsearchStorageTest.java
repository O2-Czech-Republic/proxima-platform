/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.elasticsearch;

import static cz.o2.proxima.storage.internal.AbstractDataAccessorFactory.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import java.net.URI;
import org.junit.jupiter.api.Test;

class ElasticsearchStorageTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-es.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Regular<Float> metric = Regular.of(gateway, gateway.getAttribute("metric"));

  @Test
  public void testAccept() {
    ElasticsearchStorage storage = new ElasticsearchStorage();
    assertEquals(Accept.ACCEPT, storage.accepts(URI.create("elastic://asdas")));
    assertEquals(Accept.ACCEPT, storage.accepts(URI.create("elasticsearch://asdas")));
    assertEquals(Accept.REJECT, storage.accepts(URI.create("es://asdas")));
  }

  @Test
  public void testWriterToJson() {
    ElasticsearchStorage storage = new ElasticsearchStorage();
    ElasticsearchAccessor accessor =
        storage.createAccessor(direct, repo.getFamilyByName("gateway-to-es"));
    ElasticsearchWriter writer =
        (ElasticsearchWriter) Optionals.get(accessor.getWriter(direct.getContext()));
    StreamElement element = metric.upsert("key", System.currentTimeMillis(), 1.0f);
    String json = writer.toJson(element);
    JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
    assertEquals("key", obj.get("key").getAsString());
    assertEquals("gateway", obj.get("entity").getAsString());
    assertEquals("metric", obj.get("attribute").getAsString());
    assertTrue(obj.has("timestamp"));
    assertTrue(obj.has("updated_at"));
    assertTrue(obj.has("uuid"));
    assertEquals(1.0f, obj.get("data").getAsFloat(), 0.0001);

    assertEquals("key:metric", writer.toEsKey(element));
  }
}
