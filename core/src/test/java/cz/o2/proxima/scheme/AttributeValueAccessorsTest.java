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

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.scheme.AttributeValueAccessor.Type;
import cz.o2.proxima.scheme.AttributeValueAccessors.ArrayValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.PrimitiveValueAccessor;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValue;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class AttributeValueAccessorsTest {

  @Test
  public void testAccessorsSerializable() throws IOException, ClassNotFoundException {
    PrimitiveValueAccessor<Integer, String> accessor =
        PrimitiveValueAccessor.of(Object::toString, Integer::parseInt);

    assertEquals(8, (int) Optional.ofNullable(accessor.createFrom("8")).orElse(-1));
    assertEquals("8", accessor.valueOf(8));

    PrimitiveValueAccessor<Integer, String> deserialized =
        TestUtils.deserializeObject(TestUtils.serializeObject(accessor));

    assertEquals(8, (int) Optional.ofNullable(deserialized.createFrom("8")).orElse(-1));
    assertEquals("8", deserialized.valueOf(8));
  }

  @Test
  public void testPrimitiveAccessor() {
    final PrimitiveValueAccessor<String, String> accessor =
        PrimitiveValueAccessor.of(UnaryFunction.identity(), UnaryFunction.identity());

    assertEquals("foo", accessor.valueOf("foo"));
    assertEquals("bar", accessor.createFrom("bar"));
  }

  @Test
  public void testByteArrayAccessor() {
    ArrayValueAccessor<byte[], byte[]> accessor =
        ArrayValueAccessor.of(
            PrimitiveValueAccessor.of(UnaryFunction.identity(), UnaryFunction.identity()));
    assertEquals(Type.ARRAY, accessor.getType());
    final List<byte[]> expected =
        Arrays.asList(
            "foo".getBytes(StandardCharsets.UTF_8), "bar".getBytes(StandardCharsets.UTF_8));
    assertEquals(expected, accessor.valueOf(expected));
    assertEquals(expected, accessor.createFrom(expected));
  }

  @Test
  public void testArrayWithStructureValue() {
    ArrayValueAccessor<Map<String, Object>, StructureValue> accessor =
        ArrayValueAccessor.of(new TestStructureAccessor());

    assertEquals(Type.ARRAY, accessor.getType());
    assertEquals(Type.STRUCTURE, accessor.getValueAccessor().getType());
    final Map<String, Object> expected =
        new HashMap<String, Object>() {
          {
            put("field1", "value of field1");
            put("field2", 8);
          }
        };

    assertEquals(
        Collections.singletonList(expected), accessor.valueOf(Collections.singletonList(expected)));
    assertEquals(
        Collections.singletonList(expected),
        accessor.createFrom(Collections.singletonList(StructureValue.of(expected))));
  }

  private static class TestStructureAccessor
      implements StructureValueAccessor<Map<String, Object>> {

    @Override
    public StructureValue valueOf(Map<String, Object> object) {
      return StructureValue.of(object);
    }

    @Override
    public Map<String, Object> createFrom(StructureValue object) {
      return object.value();
    }
  }
}
