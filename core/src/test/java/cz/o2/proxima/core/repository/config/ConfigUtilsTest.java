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
package cz.o2.proxima.core.repository.config;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import org.junit.Test;

/** Test {@link ConfigUtils}. */
public class ConfigUtilsTest {

  @Test
  public void testStorageReplacement() {
    Config replaced =
        ConfigUtils.withStorageReplacement(
            ConfigFactory.load("test-reference.conf").resolve(),
            name -> true,
            name -> URI.create(String.format("inmem:///%s", name)));
    Repository repo = Repository.ofTest(replaced);
    repo.getAllFamilies()
        .filter(af -> !af.isProxy())
        .forEach(
            af ->
                assertEquals(
                    "Invalid storage in " + af,
                    String.format("inmem:///%s", af.getName()),
                    af.getStorageUri().toString()));
  }
}
