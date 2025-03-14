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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import cz.o2.proxima.core.repository.ConfigConstants;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Partitioner;
import cz.o2.proxima.core.transaction.Request.Flags;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import org.junit.Test;

public class TransactionPartitionerTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-transactions.conf"));
  private final EntityDescriptor transaction = repo.getEntity(ConfigConstants.TRANSACTION_ENTITY);
  private final Wildcard<Response> response =
      Wildcard.of(transaction, transaction.getAttribute(ConfigConstants.RESPONSE_ATTRIBUTE));
  private final Wildcard<Request> request =
      Wildcard.of(transaction, transaction.getAttribute(ConfigConstants.REQUEST_ATTRIBUTE));

  private final Partitioner partitioner = new TransactionPartitioner();

  @Test
  public void testPartitioner() {
    Request r = Request.builder().flags(Flags.COMMIT).responsePartitionId(1).build();
    StreamElement responseUpsert =
        response.upsert(
            "t", "commit", System.currentTimeMillis(), Response.forRequest(r).committed());
    assertEquals(1, partitioner.getPartitionId(responseUpsert));
    StreamElement otherUpsert = request.upsert("t", "commit", System.currentTimeMillis(), r);
    assertNotEquals(1, partitioner.getPartitionId(otherUpsert));
  }
}
