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
package cz.o2.proxima.tools.groovy.util;

import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import cz.o2.proxima.tools.groovy.Console;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class ConsoleRandomReaderTest {

  private final Config config = ConfigFactory.load("test-reference.conf").resolve();
  private final Repository repo = Repository.ofTest(config);
  private final Console console = Console.create(config, repo);
  private final EntityDescriptor proxied = repo.getEntity("proxied");
  private final AttributeDescriptor<?> ints = proxied.getAttribute("ints.*");

  @Test(timeout = 10000)
  public void testConsoleRandomReaderRead() throws InterruptedException {
    console.put(proxied, ints, "key", "ints.1", "1");
    KeyValue<Integer> result =
        console.getRandomAccessReader(proxied.getName()).get("key", "ints.1");
    assertNotNull(result.getValue());
    assertEquals(1, (int) result.getParsedRequired());

    List<Pair<RandomOffset, String>> keys = new ArrayList<>();
    console.getRandomAccessReader(proxied.getName()).listKeys(keys::add);
    assertEquals(
        Collections.singletonList("key"),
        keys.stream().map(Pair::getSecond).collect(Collectors.toList()));

    keys = console.getRandomAccessReader(proxied.getName()).listKeys("", -1);
    assertEquals(
        Collections.singletonList("key"),
        keys.stream().map(Pair::getSecond).collect(Collectors.toList()));
  }

  @Test
  public void testConsolePut() throws InterruptedException {
    console.put(proxied, ints, "key", "ints.1", "1");
    assertThrows(
        NumberFormatException.class, () -> console.put(proxied, ints, "key", "ints.2", "\"2\""));
  }
}
