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
package cz.o2.proxima.direct.io.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import cz.o2.proxima.core.scheme.SerializationException;
import cz.o2.proxima.internal.com.google.gson.JsonObject;
import cz.o2.proxima.internal.com.google.gson.JsonParser;
import org.junit.Test;

public class TopicOffsetExternalizerTest {

  private final TopicOffsetExternalizer externalizer = new TopicOffsetExternalizer();

  @Test
  public void testToJson() {
    String json = externalizer.toJson(new TopicOffset(new PartitionWithTopic("topic-1", 1), 1, 2));
    JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
    assertEquals("topic-1", obj.get("topic").getAsString());
    assertEquals(1, obj.get("partition").getAsInt());
    assertEquals(1, obj.get("offset").getAsLong());
    assertEquals(2, obj.get("watermark").getAsLong());
  }

  @Test
  public void testFromJson() {
    TopicOffset offset = new TopicOffset(new PartitionWithTopic("topic-1", 1), 1, 2);
    assertEquals(offset, externalizer.fromJson(externalizer.toJson(offset)));
  }

  @Test
  public void testFromBytesWhenInvalidJson() {
    assertThrows(SerializationException.class, () -> externalizer.fromJson(""));
  }

  @Test
  public void testFromBytes() {
    TopicOffset offset = new TopicOffset(new PartitionWithTopic("topic-1", 1), 1, 2);
    assertEquals(offset, externalizer.fromBytes(externalizer.toBytes(offset)));
  }

  @Test
  public void testFromBytesWhenInvalidBytes() {
    assertThrows(SerializationException.class, () -> externalizer.fromBytes(new byte[] {0x0}));
  }
}
