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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.Map;

/** Transform {@code event.data} to {@code dummy.wildcard.<stamp>}. */
public class EventDataToDummy implements ElementWiseTransformation {

  EntityDescriptor target;
  AttributeDescriptor targetAttr;
  String prefix;

  @Override
  public void setup(Repository repo, Map<String, Object> cfg) {
    target =
        repo.findEntity("dummy")
            .orElseThrow(() -> new IllegalStateException("Missing entity `dummy`"));
    targetAttr =
        target
            .findAttribute("wildcard.*")
            .orElseThrow(
                () -> new IllegalArgumentException("Missing attribute `wildcard.*` in `dummy`"));
    prefix = targetAttr.toAttributePrefix();
  }

  @Override
  public int apply(StreamElement input, Collector<StreamElement> collector) {

    collector.collect(
        StreamElement.upsert(
            target,
            targetAttr,
            input.getUuid(),
            input.getKey(),
            prefix + input.getStamp(),
            input.getStamp(),
            input.getValue()));
    return 1;
  }
}
