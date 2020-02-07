/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import java.util.Map.Entry;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
public class AttributeFamilyConsumerNameGeneratorImpl
    implements AttributeFamilyConsumerNameGenerator {

  @Getter private final String name;

  public AttributeFamilyConsumerNameGeneratorImpl(String name) {
    this.name = name;
  }

  public static AttributeFamilyConsumerNameGenerator of(AttributeFamilyDescriptor descriptor) {
    String prefix =
        descriptor
            .getCfg()
            .entrySet()
            .stream()
            .filter(
                opt ->
                    opt.getKey()
                        .equalsIgnoreCase(AttributeFamilyDescriptor.CFG_CONSUMER_NAME_SUFFIX))
            .findFirst()
            .map(Entry::getValue)
            .map(p -> String.format("-%s", p))
            .orElse("");

    return new AttributeFamilyConsumerNameGeneratorImpl(descriptor.getName() + prefix);
  }
}
