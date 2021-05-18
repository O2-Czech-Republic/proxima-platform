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

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
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
            gateway,
            status,
            UUID.randomUUID().toString(),
            "key",
            status.getName(),
            0L,
            new byte[] {1, 2, 3});
    StreamElement delete =
        StreamElement.delete(
            gateway, status, UUID.randomUUID().toString(), "key", status.getName(), 1L);
    Commit commit = Commit.of(1L, 1234567890000L, Arrays.asList(upsert, delete));
    assertEquals(1L, commit.getSeqId());
    assertEquals(1234567890000L, commit.getStamp());
    assertEquals(2, commit.getUpdates().size());
    assertFalse(commit.getUpdates().get(0).isDelete());
    assertTrue(commit.getUpdates().get(1).isDelete());
    for (int i = 0; i < 2; i++) {
      assertEquals(1L, commit.getUpdates().get(i).getSequentialId());
      assertEquals(1234567890000L, commit.getUpdates().get(i).getStamp());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWildcardDeleteUnsupported() {
    Commit.of(
        1L,
        System.currentTimeMillis(),
        Collections.singletonList(
            StreamElement.deleteWildcard(
                gateway, device, UUID.randomUUID().toString(), "key", 1L)));
  }
}
