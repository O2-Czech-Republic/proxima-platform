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

import cz.o2.proxima.core.util.ExceptionUtils;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import org.junit.Test;

public class FieldExtractorTest {

  // Define the source class which we will use to provide field values
  public static class SourceClass {
    private final String name;
    private final Integer value;

    public SourceClass(String name, int value) {
      this.name = name;
      this.value = value;
    }
  }

  @Test
  public void test() throws Exception {

    // Instantiate the source object whose fields will be copied
    SourceClass sourceInstance = new SourceClass("TestName", 42);

    // Use Byte Buddy to generate a dynamic class
    Class<?> dynamicClass;
    dynamicClass =
        new ByteBuddy()
            .subclass(Object.class)
            .name(SourceClass.class.getPackageName() + ".GeneratedClass")

            // Define fields to match the source instance fields
            .defineField("name", String.class, Visibility.PRIVATE, FieldManifestation.FINAL)
            .defineField("value", Integer.class, Visibility.PRIVATE, FieldManifestation.FINAL)

            // Define a constructor that takes an instance of SourceClass
            .defineConstructor(Visibility.PUBLIC)
            .withParameters(SourceClass.class, Integer.class /* to be ignored */)
            .intercept(
                MethodCall.invoke(
                        ExceptionUtils.uncheckedFactory(() -> Object.class.getConstructor()))
                    .andThen(new Implementation.Simple(new FieldExtractor(SourceClass.class))))
            .make()
            .load(FieldExtractorTest.class.getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
            .getLoaded();

    // Instantiate the generated class using the constructor that takes SourceClass as parameter
    Constructor<?> constructor = dynamicClass.getConstructor(SourceClass.class, Integer.class);
    Object generatedInstance = constructor.newInstance(sourceInstance, 0);

    // Verify that fields are copied correctly
    for (Field field : dynamicClass.getDeclaredFields()) {
      field.setAccessible(true);
      Field source = SourceClass.class.getDeclaredField(field.getName());
      assertEquals(source.get(sourceInstance), field.get(generatedInstance));
    }
  }
}
