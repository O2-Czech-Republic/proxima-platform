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

import static org.junit.Assert.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.RepositoryFactory.VersionedCaching;
import org.junit.Test;

public class RepositoryFactoryTest {

  private final Config config =
      ConfigFactory.load()
          .withFallback(ConfigFactory.load("test-replication.conf"))
          .withFallback(ConfigFactory.load("test-reference.conf"))
          .resolve();

  @Test
  public void testMemoryEfficientFactory() {
    Repository first = Repository.of(config);
    Repository second = RepositoryFactory.compressed(config).apply();
    assertNotSame(first, second);
    assertEquals(first, second);
  }

  @Test
  public void testCachedFactoryCaches() {
    Repository repo = Repository.of(config);
    VersionedCaching.drop();
    repo = repo.asFactory().apply();
    assertSame(repo, repo.asFactory().apply());
  }
}
