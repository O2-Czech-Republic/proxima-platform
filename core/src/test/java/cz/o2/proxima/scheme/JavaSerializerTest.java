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

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import lombok.Data;
import org.junit.Before;
import org.junit.Test;

/** Test for ${@link JavaSerializer}. */
public class JavaSerializerTest {
  private final ValueSerializerFactory factory = new JavaSerializer();
  private ValueSerializer<MyTestObject> serializer;

  @Before
  public void setup() throws URISyntaxException {
    serializer =
        factory.getValueSerializer(
            new URI("java:cz.o2.proxima.scheme.JavaSerializerTest$MyTestObject"));
  }

  @Test
  public void testSerializeAndDeserializeCustomClass() {
    MyTestObject v = new MyTestObject();
    v.setKey("test-key");
    v.setValue(8);

    byte[] bytes = serializer.serialize(v);
    Optional<MyTestObject> d = serializer.deserialize(bytes);
    assertTrue(d.isPresent());
    assertEquals("test-key", d.get().getKey());
    assertEquals(8, d.get().getValue());
    assertEquals(v, d.get());
  }

  @Test
  public void testIsValidWithCustomClass() {
    assertTrue(serializer.isValid(new byte[] {}));
    assertNotNull(serializer.getDefault());
    assertTrue(serializer.isUsable());
  }

  @Test
  public void testDefaultWithCustomClass() {
    assertEquals(new MyTestObject(), serializer.getDefault());
  }

  @Test
  public void testGetClassNameWithCustomClass() throws URISyntaxException {
    assertEquals(
        MyTestObject.class.getSimpleName(), factory.getClassName(new URI("java:MyTestObject")));
  }

  @Test
  public void testSerializerForStringClassWithPackage() throws URISyntaxException {
    ValueSerializer<String> s = factory.getValueSerializer(new URI("java:java.lang.String"));
    String value = "my-super-value";
    byte[] serialized = s.serialize(value);
    Optional<String> deserialized = s.deserialize(serialized);
    assertTrue(deserialized.isPresent());
    assertEquals(value, deserialized.get());

    assertEquals("", s.getDefault());
    assertEquals("java.lang.String", factory.getClassName(new URI("java:java.lang.String")));
  }

  @Test
  public void testSerializerForStringClass() throws URISyntaxException {
    ValueSerializer<String> s = factory.getValueSerializer(new URI("java:String"));
    String value = "my-super-value";
    byte[] serialized = s.serialize(value);
    Optional<String> deserialized = s.deserialize(serialized);
    assertTrue(deserialized.isPresent());
    assertEquals(value, deserialized.get());

    assertEquals("", s.getDefault());
    assertEquals("String", factory.getClassName(new URI("java:String")));
  }

  @Test(expected = SerializationException.class)
  public void testWithNotExistsClass() throws URISyntaxException {
    ValueSerializer s = factory.getValueSerializer(new URI("java:not-exists"));
    s.isValid(new byte[] {});
  }

  @Data
  public static class MyTestObject implements Serializable {
    private static final long serialVersionUID = -8346429941665548558L;
    String key;
    int value;
  }
}
