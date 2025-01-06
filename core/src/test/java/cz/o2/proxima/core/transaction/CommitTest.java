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
package cz.o2.proxima.core.transaction;

import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.junit.Test;

public class CommitTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf"));
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<?> status = gateway.getAttribute("status");
  private final AttributeDescriptor<?> device = gateway.getAttribute("device.*");

  @Test
  public void testCreation() {
    StreamElement upsert =
        StreamElement.upsert(
            gateway, status, 1L, "key", status.getName(), 1L, new byte[] {1, 2, 3});
    StreamElement delete = StreamElement.delete(gateway, status, 1L, "key", status.getName(), 1L);
    Commit commit = Commit.outputs(Collections.emptyList(), Arrays.asList(upsert, delete));
    assertEquals(2, commit.getOutputs().size());
    assertFalse(Iterables.get(commit.getOutputs(), 0).isDelete());
    assertTrue(Iterables.get(commit.getOutputs(), 1).isDelete());
    assertTrue(commit.getTransactionUpdates().isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWildcardDeleteUnsupported() {
    Commit.outputs(
        Collections.emptyList(),
        Collections.singletonList(
            StreamElement.deleteWildcard(
                gateway, device, UUID.randomUUID().toString(), "key", 1L)));
  }
}
