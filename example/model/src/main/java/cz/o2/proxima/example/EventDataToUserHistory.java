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
package cz.o2.proxima.example;

import cz.o2.proxima.example.event.Event;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transform.ElementWiseTransformation;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** Transformation function from {@code event.data} to {@code user.event.<stamp>}. */
@Slf4j
public class EventDataToUserHistory implements ElementWiseTransformation {

  EntityDescriptor user;
  AttributeDescriptor<Event.BaseEvent> event;
  String prefix;

  @SuppressWarnings("unchecked")
  @Override
  public void setup(Repository repo, Map<String, Object> cfg) {
    user =
        repo.findEntity("user")
            .orElseThrow(() -> new IllegalArgumentException("No entity named `user` found"));
    event =
        (AttributeDescriptor)
            user.findAttribute("event.*")
                .orElseThrow(
                    () -> new IllegalArgumentException("No attribute `event.*` found in `user`"));
    prefix = event.toAttributePrefix();
  }

  @Override
  public int apply(StreamElement input, Collector<StreamElement> collector) {
    if (!input.isDelete()) {
      Optional<Event.BaseEvent> data = input.getParsed();
      if (data.isPresent()) {
        collector.collect(
            StreamElement.upsert(
                user,
                event,
                input.getUuid(),
                data.get().getUserName(),
                prefix + input.getStamp(),
                input.getStamp(),
                input.getValue()));
        return 1;
      }
    } else {
      log.warn("Ignored delete in transformed event {}", input);
    }
    return 0;
  }
}
