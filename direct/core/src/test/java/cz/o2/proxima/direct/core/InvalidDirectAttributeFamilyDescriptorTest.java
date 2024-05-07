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
package cz.o2.proxima.direct.core;

import static org.junit.Assert.assertThrows;

import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import org.junit.Test;

public class InvalidDirectAttributeFamilyDescriptorTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());

  @Test
  public void testThrows() {
    InvalidDirectAttributeFamilyDescriptor desc =
        new InvalidDirectAttributeFamilyDescriptor(
            repo, repo.getFamilyByName("proxy-event-storage"));
    assertThrows(IllegalArgumentException.class, desc::getBatchReader);
    assertThrows(IllegalArgumentException.class, desc::getCommitLogReader);
    assertThrows(IllegalArgumentException.class, desc::getCachedView);
    assertThrows(IllegalArgumentException.class, desc::getRandomAccessReader);
    assertThrows(IllegalArgumentException.class, desc::getWriter);
    assertThrows(IllegalArgumentException.class, desc::hasBatchReader);
    assertThrows(IllegalArgumentException.class, desc::hasCachedView);
    assertThrows(IllegalArgumentException.class, desc::hasCommitLogReader);
    assertThrows(IllegalArgumentException.class, desc::hasRandomAccessReader);
  }
}
