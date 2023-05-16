/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.PassthroughFilter;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.Response.Flags;
import cz.o2.proxima.core.transform.ElementWiseTransformation;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedRuntimeException;
import cz.o2.proxima.direct.transform.TransformationObserver.Contextual;
import cz.o2.proxima.direct.transform.TransformationObserver.NonContextual;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class TransformationObserverTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Regular<byte[]> armed = Regular.of(gateway, gateway.getAttribute("armed"));

  @Test
  public void testContextualTransformCallsOnReplicated() {
    AtomicInteger failedCnt = new AtomicInteger();
    DirectElementWiseTransform transform =
        new DirectElementWiseTransform() {
          @Override
          public void setup(
              Repository repo, DirectDataOperator directDataOperator, Map<String, Object> cfg) {}

          @Override
          public void transform(StreamElement input, CommitCallback commit) {
            commit.commit(true, null);
          }

          @Override
          public void close() {}
        };

    List<StreamElement> replicated = new ArrayList<>();
    Contextual observer =
        new Contextual(direct, "name", transform, false, new PassthroughFilter()) {
          @Override
          protected void onReplicated(StreamElement element) {
            replicated.add(element);
          }
        };
    observer.doTransform(
        armed.upsert("key", System.currentTimeMillis(), new byte[] {}),
        (succ, exc) -> {
          assertTrue(succ);
        });
    assertEquals(1, replicated.size());
  }

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
  public void testContextualTransactionRejectedExceptionHandlingFailed() {
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

  @Test
  public void testNonContextualTransactionRejectedExceptionHandlingFailed()
      throws InterruptedException {
    StreamElement element = armed.upsert("key", System.currentTimeMillis(), new byte[] {});
    ElementWiseTransformation transform =
        new ElementWiseTransformation() {
          @Override
          public void setup(Repository repo, Map<String, Object> cfg) {}

          @Override
          public int apply(StreamElement input, Collector<StreamElement> collector) {
            collector.collect(element);
            return 1;
          }
        };
    NonContextual observer =
        new NonContextual(direct, "name", transform, true, new PassthroughFilter()) {
          @Override
          OnlineAttributeWriter getOnlineWriterFor(StreamElement elem) {
            return throwingWriter();
          }
        };
    BlockingQueue<Optional<Throwable>> commitQueue = new ArrayBlockingQueue<>(1);
    observer.doTransform(element, (succ, exc) -> commitQueue.offer(Optional.ofNullable(exc)));
    Optional<Throwable> thrown = commitQueue.take();
    assertTrue(thrown.isPresent());
    assertTrue(
        "Expected TransactionRejectedException, got " + thrown.get(),
        thrown.get() instanceof TransactionRejectedException);
  }

  @Test
  public void testNonContextualTransactionRejectedExceptionHandlingSuccess()
      throws InterruptedException {
    AtomicInteger fails = new AtomicInteger();
    StreamElement element = armed.upsert("key", System.currentTimeMillis(), new byte[] {});
    ElementWiseTransformation transform =
        new ElementWiseTransformation() {
          @Override
          public void setup(Repository repo, Map<String, Object> cfg) {}

          @Override
          public int apply(StreamElement input, Collector<StreamElement> collector) {
            collector.collect(element);
            return 1;
          }
        };
    NonContextual observer =
        new NonContextual(direct, "name", transform, true, new PassthroughFilter()) {
          @Override
          OnlineAttributeWriter getOnlineWriterFor(StreamElement elem) {
            if (fails.incrementAndGet() < 3) {
              return throwingWriter();
            }
            return super.getOnlineWriterFor(elem);
          }
        };
    BlockingQueue<Optional<Throwable>> commitQueue = new ArrayBlockingQueue<>(1);
    observer.doTransform(element, (succ, exc) -> commitQueue.offer(Optional.ofNullable(exc)));
    Optional<Throwable> thrown = commitQueue.take();
    assertFalse(thrown.isPresent());
  }

  private OnlineAttributeWriter throwingWriter() {
    return new OnlineAttributeWriter() {
      @Override
      public void write(StreamElement data, CommitCallback statusCallback) {
        statusCallback.commit(false, new TransactionRejectedException("t", Flags.ABORTED) {});
      }

      @Override
      public Factory<? extends OnlineAttributeWriter> asFactory() {
        return repo -> throwingWriter();
      }

      @Override
      public URI getUri() {
        return URI.create("throwing-writer:///");
      }

      @Override
      public void close() {}
    };
  }
}
