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
package cz.o2.proxima.transform;

import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * A stateless transformation that performs transformation from one attribute to another. This
 * {@link Transformation} is used primarily when remapping two attributes one-to-one where only
 * change in name and {@link AttributeDescriptor} occurs. Typical scenario involved replications and
 * proxy attributes where reads and/or writes are routed to different atttributes without changing
 * the payload (note that scheme serializer might change).
 */
@Evolving
@Slf4j
public class RenameTransformation implements ElementWiseTransformation {

  private UnaryFunction<AttributeDescriptor<?>, AttributeDescriptor<?>> descTransform;
  private BiFunction<String, AttributeDescriptor<?>, String> nameTransform;

  protected RenameTransformation() {}

  public RenameTransformation(
      UnaryFunction<AttributeDescriptor<?>, AttributeDescriptor<?>> descTransform,
      BiFunction<String, AttributeDescriptor<?>, String> nameTransform) {

    this.descTransform = descTransform;
    this.nameTransform = nameTransform;
  }

  @Override
  public void setup(Repository repo, Map<String, Object> cfg) {
    // nop
  }

  /**
   * Update desc and name transforms. This is typically used in response to {@link
   * #setup(Repository, Map)}.
   *
   * @param descTransform transformation
   * @param nameTransform name transformation
   */
  protected void setTransforms(
      UnaryFunction<AttributeDescriptor<?>, AttributeDescriptor<?>> descTransform,
      BiFunction<String, AttributeDescriptor<?>, String> nameTransform) {

    this.descTransform = descTransform;
    this.nameTransform = nameTransform;
  }

  @Override
  public int apply(StreamElement input, Collector<StreamElement> collector) {
    @SuppressWarnings("unchecked")
    AttributeDescriptor<Object> desc =
        (AttributeDescriptor<Object>) descTransform.apply(input.getAttributeDescriptor());

    final StreamElement collected;
    if (desc != null) {
      if (input.isDeleteWildcard()) {
        collected =
            StreamElement.deleteWildcard(
                input.getEntityDescriptor(),
                desc,
                input.getUuid(),
                input.getKey(),
                nameTransform.apply(input.getAttribute(), input.getAttributeDescriptor()),
                input.getStamp());
      } else if (input.isDelete()) {
        collected =
            StreamElement.delete(
                input.getEntityDescriptor(),
                desc,
                input.getUuid(),
                input.getKey(),
                nameTransform.apply(input.getAttribute(), input.getAttributeDescriptor()),
                input.getStamp());
      } else {
        collected =
            StreamElement.upsert(
                input.getEntityDescriptor(),
                desc,
                input.getUuid(),
                input.getKey(),
                nameTransform.apply(input.getAttribute(), input.getAttributeDescriptor()),
                input.getStamp(),
                remapPayload(input, desc));
      }
      collector.collect(collected);
      return 1;
    }
    return 0;
  }

  private byte[] remapPayload(StreamElement input, AttributeDescriptor<Object> target) {
    return input
        .getAttributeDescriptor()
        .getValueSerializer()
        .deserialize(input.getValue())
        .map(obj -> target.getValueSerializer().serialize(obj))
        .orElseThrow(() -> new IllegalArgumentException("Cannot deserialize payload of " + input));
  }
}
