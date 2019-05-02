/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
import org.junit.Test;

/**
 * Test suite for {@link StringUtf8Serializer}.
 */
public class StringUtf8SerializerTest {

  private final StringUtf8Serializer serializer = new StringUtf8Serializer();

  @Test
  public void testScheme() {
    assertEquals("string", serializer.getAcceptableScheme());
  }

  @Test
  public void testClassName() {
    assertEquals("String", serializer.getClassName(null));
  }

  @Test
  public void testSerializeDeserialize() {
    ValueSerializer<String> s = serializer.getValueSerializer(null);
    assertEquals("blah", s.deserialize(s.serialize("blah")).get());
  }

  @Test
  public void testJsonSerializeDeserialize() {
    ValueSerializer<String> s = serializer.getValueSerializer(null);
    assertEquals("blah", s.fromJsonValue(s.asJsonValue("blah")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJsonSerializeDeserializeInvalid() {
    ValueSerializer<String> s = serializer.getValueSerializer(null);
    assertEquals("blah", s.fromJsonValue(s.asJsonValue("blah") + "."));
  }

  @Test
  public void testDefaultValue() {
    ValueSerializer<String> s = serializer.getValueSerializer(null);
    assertEquals("", s.getDefault());
  }

}
