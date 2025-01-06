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
package cz.o2.proxima.beam.util.state;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.function.Predicate;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;

public class FieldExtractor implements Implementation, ByteCodeAppender {

  private final Class<?> sourceClass;
  private final Predicate<Field> includePredicate;

  public FieldExtractor(Class<?> sourceClass) {
    this(sourceClass, f -> !Modifier.isStatic(f.getModifiers()));
  }

  public FieldExtractor(Class<?> sourceClass, Predicate<Field> includePredicate) {
    this.sourceClass = sourceClass;
    this.includePredicate = includePredicate;
  }

  @Override
  public Size apply(
      MethodVisitor methodVisitor,
      Context implementationContext,
      MethodDescription instrumentedMethod) {

    String targetClass = implementationContext.getInstrumentedType().getInternalName();

    // For each field in SourceClass, copy the value to the generated class
    for (Field field : sourceClass.getDeclaredFields()) {
      if (includePredicate.test(field)) {
        field.setAccessible(true);

        // Load `this` onto the stack for setting the field
        methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);

        // Use reflection to get the Field object from SourceClass
        // Step 1: Load the class name of source class onto the stack
        methodVisitor.visitLdcInsn(TypeDescription.ForLoadedType.of(sourceClass).getName());

        // Step 2: Invoke Class.forName(String) to get the Class object of SourceClass
        methodVisitor.visitMethodInsn(
            Opcodes.INVOKESTATIC,
            "java/lang/Class",
            "forName",
            "(Ljava/lang/String;)Ljava/lang/Class;",
            false);

        // Step 3: Load the field name as a constant onto the stack
        methodVisitor.visitLdcInsn(field.getName());

        // Step 4: Invoke getDeclaredField(String) on the Class object to get the Field object
        methodVisitor.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            "java/lang/Class",
            "getDeclaredField",
            "(Ljava/lang/String;)Ljava/lang/reflect/Field;",
            false);

        // Step 5: Duplicate the Field object on the stack (so we have two copies of it)
        methodVisitor.visitInsn(Opcodes.DUP);

        // Step 6: Set the Field accessible (iconst_1 for true)
        methodVisitor.visitInsn(Opcodes.ICONST_1);
        methodVisitor.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL, "java/lang/reflect/Field", "setAccessible", "(Z)V", false);

        // Step 7: Load the source instance to get the field value
        methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);

        // Step 8: Invoke Field.get(Object) to retrieve the field value from the source instance
        methodVisitor.visitMethodInsn(
            Opcodes.INVOKEVIRTUAL,
            "java/lang/reflect/Field",
            "get",
            "(Ljava/lang/Object;)Ljava/lang/Object;",
            false);

        // Step 9: Cast the value to the correct field type if necessary
        if (!field.getType().isPrimitive()) {
          // Use CHECKCAST to cast the returned Object to the correct type
          methodVisitor.visitTypeInsn(
              Opcodes.CHECKCAST,
              TypeDescription.ForLoadedType.of(field.getType()).getInternalName());
        }

        // Step 10: Set the value to the corresponding field in the generated class
        methodVisitor.visitFieldInsn(
            Opcodes.PUTFIELD,
            targetClass,
            field.getName(),
            TypeDescription.ForLoadedType.of(field.getType()).getDescriptor());
      }
    }

    // Add the return statement to end the constructor properly
    methodVisitor.visitInsn(Opcodes.RETURN);

    return new ByteCodeAppender.Size(4, instrumentedMethod.getStackSize());
  }

  @Override
  public ByteCodeAppender appender(Target target) {
    return this;
  }

  @Override
  public InstrumentedType prepare(InstrumentedType instrumentedType) {
    return instrumentedType;
  }
}
