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
package cz.o2.proxima.example.model;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.example.event.Event.BaseEvent;
import cz.o2.proxima.example.user.User.Details;
import cz.o2.proxima.storage.StreamElement;
import java.time.Instant;
import org.junit.Test;

public class ModelTest {

  private Model model = Model.of(() -> ConfigFactory.load().resolve());

  @Test
  public void testSimplifiedUpsertFromGeneratedSource() {
    StreamElement element =
        model
            .getUser()
            .getDetailsDescriptor()
            .upsert("key", Instant.now(), Details.newBuilder().setEmail("email").build());
    assertNotNull(element);
  }

  @Test
  public void testSimplifiedUpsertWildcard() {
    StreamElement element =
        model
            .getUser()
            .getEventDescriptor()
            .upsert("user", "key", "suffix", Instant.now(), BaseEvent.newBuilder().build());
    assertEquals("key", element.getKey());
  }
}
