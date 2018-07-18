/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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

package cz.o2.proxima.repository;

import cz.o2.proxima.annotations.Internal;
import java.net.URI;
import cz.o2.proxima.scheme.ValueSerializer;

import javax.annotation.Nullable;

/**
 * Descriptor of attribute of entity.
 */
@Internal
public class AttributeDescriptorImpl<T>
    extends AttributeDescriptorBase<T> {

  AttributeDescriptorImpl(
      String name, String entity,
      URI schemeURI, @Nullable ValueSerializer<T> serializer,
      boolean replica) {

    super(name, entity, schemeURI, serializer, replica);
  }


  @Override
  public String toString() {
    return "AttributeDescriptor(entity=" + entity + ", name=" + name + ")";
  }

}
