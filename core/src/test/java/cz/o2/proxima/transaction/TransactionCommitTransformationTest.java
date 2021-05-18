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

import static org.junit.Assert.assertEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class TransactionCommitTransformationTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-transactions.conf"));
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<?> status = gateway.getAttribute("status");
  private final AttributeDescriptor<?> device = gateway.getAttribute("device.*");
  private final EntityDescriptor transaction = repo.getEntity("_transaction");
  private final AttributeDescriptor<Commit> commitDesc = transaction.getAttribute("commit");
  private TransactionCommitTransformation transformation;

  @Before
  public void setUp() {
    transformation = new TransactionCommitTransformation();
    transformation.setup(repo, Collections.emptyMap());
  }

  @Test
  public void testTransform() {
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
    List<StreamElement> outputs = new ArrayList<>();
    int expected =
        transformation.apply(
            StreamElement.upsert(
                transaction,
                commitDesc,
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                commitDesc.getName(),
                System.currentTimeMillis(),
                commitDesc.getValueSerializer().serialize(commit)),
            outputs::add);
    assertEquals(2, expected);
    assertEquals(2, outputs.size());
    assertEquals(commit.getUpdates().get(0), outputs.get(0));
    assertEquals(commit.getUpdates().get(1), outputs.get(1));
  }
}
