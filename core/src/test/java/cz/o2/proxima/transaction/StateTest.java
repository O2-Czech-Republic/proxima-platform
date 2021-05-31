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
package cz.o2.proxima.transaction;

import static org.junit.Assert.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.util.Collections;
import org.junit.Test;

/** Test {@link State} transitions. */
public class StateTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-transactions.conf"));
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final AttributeDescriptor<byte[]> device = gateway.getAttribute("device.*");

  @Test
  public void testStateTransitions() {
    State s = State.open(5L, 1234567890000L, Collections.emptyList());
    assertEquals(5L, s.getSequentialId());
    assertEquals(1234567890000L, s.getStamp());
    assertTrue(s.getInputAttributes().isEmpty());
    KeyAttribute ka = KeyAttributes.ofAttributeDescriptor(gateway, "key", status, 2L);
    s = s.update(Lists.newArrayList(ka));
    assertEquals(5L, s.getSequentialId());
    assertEquals(1234567890000L, s.getStamp());
    assertEquals(1, s.getInputAttributes().size());
    assertEquals(ka, Iterables.get(s.getInputAttributes(), 0));
    ka = KeyAttributes.ofAttributeDescriptor(gateway, "key", device, 3L, "1");
    State committed = s.committed(Collections.singletonList(ka));
    assertEquals(1, committed.getInputAttributes().size());
    assertEquals(1, committed.getCommittedAttributes().size());
    assertNotEquals(
        Iterables.get(committed.getInputAttributes(), 0),
        Iterables.get(committed.getCommittedAttributes(), 0));
    assertEquals(ka, Iterables.get(committed.getCommittedAttributes(), 0));
    assertEquals(5L, committed.getSequentialId());
    assertEquals(1234567890000L, s.getStamp());
    State aborted = s.aborted();
    assertEquals(5L, aborted.getSequentialId());
    assertEquals(1234567890000L, s.getStamp());
  }
}
