/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.transform;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedRuntimeException;
import cz.o2.proxima.direct.transform.TransformationObserver.Contextual;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.Response.Flags;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class TransformationObserverTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Regular<byte[]> armed = Regular.of(gateway, gateway.getAttribute("armed"));

  @Test
  public void testTransactionRejectedExceptionHandling() {
    AtomicInteger failedCnt = new AtomicInteger();
    DirectElementWiseTransform transform =
        new DirectElementWiseTransform() {
          @Override
          public void setup(
              Repository repo, DirectDataOperator directDataOperator, Map<String, Object> cfg) {}

          @Override
          public void transform(StreamElement input, CommitCallback commit) {
            if (failedCnt.incrementAndGet() < 3) {
              throw new TransactionRejectedRuntimeException(
                  new TransactionRejectedException("t", Flags.ABORTED) {});
            }
          }

          @Override
          public void close() {}
        };
    Contextual observer = new Contextual(direct, "name", transform, true, new PassthroughFilter());
    observer.doTransform(
        armed.upsert("key", System.currentTimeMillis(), new byte[] {}),
        (succ, exc) -> {
          assertTrue(succ);
        });
  }

  @Test
  public void testTransactionRejectedExceptionHandlingFailed() {
    DirectElementWiseTransform transform =
        new DirectElementWiseTransform() {
          @Override
          public void setup(
              Repository repo, DirectDataOperator directDataOperator, Map<String, Object> cfg) {}

          @Override
          public void transform(StreamElement input, CommitCallback commit) {
            throw new TransactionRejectedRuntimeException(
                new TransactionRejectedException("t", Flags.ABORTED) {});
          }

          @Override
          public void close() {}
        };
    Contextual observer = new Contextual(direct, "name", transform, true, new PassthroughFilter());
    StreamElement element = armed.upsert("key", System.currentTimeMillis(), new byte[] {});
    assertThrows(
        TransactionRejectedRuntimeException.class,
        () -> observer.doTransform(element, (succ, exc) -> {}));
  }
}
