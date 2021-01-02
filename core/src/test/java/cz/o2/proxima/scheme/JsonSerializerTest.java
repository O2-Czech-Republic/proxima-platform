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
package cz.o2.proxima.scheme;

import static org.junit.Assert.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/** Test {@link JsonSerializer}. */
public class JsonSerializerTest {

  private final JsonSerializer serializer = new JsonSerializer();

  @Test
  public void testJsonSerializer() {
    assertEquals("json", serializer.getAcceptableScheme());
    assertEquals(String.class.getName(), serializer.getClassName(URI.create("json:///")));
    ValueSerializer<String> serializer = this.serializer.getValueSerializer(null);
    String json = "{\"key\": \"value\"}";
    assertEquals(json, serializer.asJsonValue(json));
    assertEquals(json, serializer.fromJsonValue(json));
    assertArrayEquals(json.getBytes(StandardCharsets.UTF_8), serializer.serialize(json));
    assertEquals(json, serializer.deserialize(json.getBytes(StandardCharsets.UTF_8)).orElse(null));
    assertEquals("{}", serializer.getDefault());
    assertTrue(serializer.isUsable());
    assertTrue(serializer.isValid(new byte[] {}));
  }
}
