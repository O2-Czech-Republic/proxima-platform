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

import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.StateId;

class TypeId {

  private static final TypeId TIMESTAMP_TYPE =
      TypeId.of(AnnotationDescription.Builder.ofType(DoFn.Timestamp.class).build());

  private static final TypeId ELEMENT_TYPE =
      TypeId.of(AnnotationDescription.Builder.ofType(DoFn.Element.class).build());

  private static final TypeId MULTI_OUTPUT_TYPE =
      TypeId.of(TypeDescription.ForLoadedType.of(DoFn.MultiOutputReceiver.class));

  public static TypeId of(Annotation annotation) {
    return new TypeId(annotation.toString());
  }

  public static TypeId of(AnnotationDescription annotationDescription) {
    return new TypeId(annotationDescription.toString());
  }

  public static TypeId of(Type type) {
    Preconditions.checkArgument(!(type instanceof Annotation));
    return new TypeId(type.getTypeName());
  }

  public static TypeId of(TypeDefinition definition) {
    return new TypeId(definition.getTypeName());
  }

  private final String stringId;

  private TypeId(String stringId) {
    this.stringId = stringId;
  }

  @Override
  public int hashCode() {
    return stringId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof TypeId)) {
      return false;
    }
    TypeId other = (TypeId) obj;
    return this.stringId.equals(other.stringId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", stringId).toString();
  }

  public boolean isElement() {
    return equals(ELEMENT_TYPE);
  }

  public boolean isState(String stateName) {
    return equals(
        TypeId.of(
            AnnotationDescription.Builder.ofType(StateId.class)
                .define("value", stateName)
                .build()));
  }

  public boolean isTimestamp() {
    return equals(TIMESTAMP_TYPE);
  }

  public boolean isOutputReceiver(Type outputType) {
    return equals(
        TypeId.of(
            TypeDescription.Generic.Builder.parameterizedType(OutputReceiver.class, outputType)
                .build()));
  }

  public boolean isOutput(Type outputType) {
    return isOutputReceiver(outputType) || isMultiOutput();
  }

  public boolean isMultiOutput() {
    return equals(MULTI_OUTPUT_TYPE);
  }
}
