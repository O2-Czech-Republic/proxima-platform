/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.util.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import cz.o2.proxima.core.util.Optionals;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Arrays;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

public class TypeIdTest {

  @Test
  public void testAnnotations() {
    DoFn<?, ?> doFn =
        new DoFn<Object, Object>() {
          @ProcessElement
          public void process(
              @Element KV<String, Long> elem,
              @StateId("state") ValueState<Long> s,
              OutputReceiver<String> out) {}
        };

    Method process =
        Optionals.get(
            Arrays.stream(doFn.getClass().getDeclaredMethods())
                .filter(m -> m.getName().equals("process"))
                .findAny());
    Type[] parameterTypes = process.getGenericParameterTypes();
    Annotation[][] annotations = process.getParameterAnnotations();
    assertEqualType(
        TypeId.of(annotations[0][0]),
        TypeId.of(AnnotationDescription.Builder.ofType(DoFn.Element.class).build()));
    assertEqualType(
        TypeId.of(annotations[1][0]),
        TypeId.of(
            AnnotationDescription.Builder.ofType(DoFn.StateId.class)
                .define("value", "state")
                .build()));

    assertNotEqualType(
        TypeId.of(annotations[1][0]),
        TypeId.of(
            AnnotationDescription.Builder.ofType(DoFn.StateId.class)
                .define("value", "state2")
                .build()));

    assertEqualType(
        TypeId.of(parameterTypes[2]),
        TypeId.of(
            TypeDescription.Generic.Builder.parameterizedType(OutputReceiver.class, String.class)
                .build()));
  }

  private void assertNotEqualType(TypeId first, TypeId second) {
    assertNotEquals(first, second);
    assertNotEquals(first.hashCode(), second.hashCode());
  }

  private void assertEqualType(TypeId first, TypeId second) {
    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }
}
