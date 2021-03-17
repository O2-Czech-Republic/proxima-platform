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
package cz.o2.proxima.direct.transaction;

import static cz.o2.proxima.direct.commitlog.LogObserverUtils.toList;
import static org.junit.Assert.assertEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.util.Optionals;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/** Test transactions are working according to the specification. */
public class TransactionsTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-transactions.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor transaction = repo.getEntity("_transaction");
  private final Wildcard<Request> request =
      Wildcard.wildcard(transaction, transaction.getAttribute("request.*"));
  private final Wildcard<Response> response =
      Wildcard.wildcard(transaction, transaction.getAttribute("response.*"));

  @Test(expected = IllegalArgumentException.class)
  public void testDirectWriterFetchFails() {
    direct.getWriter(request);
  }

  @Test
  public void testTransactionRequestResponse() {
    // we must access the readers/writers directly through family name
    DirectAttributeFamilyDescriptor family =
        Optionals.get(direct.findFamilyByName("gateway-transaction-commit-log"));
    CommitLogReader reader = Optionals.get(family.getCommitLogReader());
    OnlineAttributeWriter writer = Optionals.get(family.getWriter()).online();
    List<Response> receivedResponses = new ArrayList<>();

    // create a simple ping-pong communication
    reader.observe(
        "requests",
        (ingest, context) -> {
          if (ingest.getAttributeDescriptor().equals(request)) {
            String requestId = request.extractSuffix(ingest.getAttribute());
            writer.write(
                response.upsert(
                    ingest.getKey(), requestId, System.currentTimeMillis(), Response.of()),
                (succ, exc) -> {
                  context.confirm();
                });
          } else {
            context.confirm();
          }
          return true;
        });

    reader.observe("responses", toList(receivedResponses, response));

    writer.write(
        request.upsert("firstTransaction", "abc", System.currentTimeMillis(), Request.of()),
        (succ, exc) -> {});

    assertEquals(1, receivedResponses.size());
  }
}
