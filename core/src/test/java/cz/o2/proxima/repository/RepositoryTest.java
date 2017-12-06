/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test repository config parsing.
 */
public class RepositoryTest {


  @Before
  public void setup() {
  }

  @After
  public void teardown() {
  }

  @Test
  public void testConfigParsing() throws IOException {
    Repository repo = Repository.Builder.of(ConfigFactory.load().resolve()).build();
    assertTrue("Entity event should have been parsed",
        repo.findEntity("event").isPresent());
    assertTrue("Entity gateway should have been parsed",
        repo.findEntity("gateway").isPresent());

    EntityDescriptor event = repo.findEntity("event").get();
    assertEquals("event", event.getName());
    assertEquals("data", event.findAttribute("data").get().getName());
    assertEquals("bytes", event.findAttribute("data").get().getSchemeURI().getScheme());
    assertNotNull(event.findAttribute("data").get().getValueSerializer());

    EntityDescriptor gateway = repo.findEntity("gateway").get();
    assertEquals("gateway", gateway.getName());
    assertEquals("bytes:///",
        gateway.findAttribute("armed").get().getSchemeURI().toString());
    assertEquals("fail:whenever",
        gateway.findAttribute("fail").get().getSchemeURI().toString());
    assertEquals("bytes:///",
        gateway.findAttribute("bytes").get().getSchemeURI().toString());
  }

}
