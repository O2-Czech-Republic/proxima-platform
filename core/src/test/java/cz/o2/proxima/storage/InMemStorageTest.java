/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test suite for {@link InMemStorage}.
 */
public class InMemStorageTest implements Serializable {

  final Repository repo = ConfigRepository.of(
      ConfigFactory.load("test-reference.conf").resolve());
  final EntityDescriptor entity = repo
      .findEntity("dummy")
      .orElseThrow(() -> new IllegalStateException("Missing entity dummy"));

  @Test(timeout = 5000)
  public void testObservePartitions()
      throws URISyntaxException, InterruptedException {

    InMemStorage storage = new InMemStorage();
    DataAccessor accessor = storage.getAccessor(
        entity, new URI("inmem:///inmemstoragetest"),
        Collections.emptyMap());
    PartitionedView view = accessor.getPartitionedView(context())
        .orElseThrow(() -> new IllegalStateException("Missing partitioned view"));
    AttributeWriterBase writer = accessor.getWriter(context())
        .orElseThrow(() -> new IllegalStateException("Missing writer"));
    SynchronousQueue<Integer> sync = new SynchronousQueue<>();
    AtomicReference<CountDownLatch> latch = new AtomicReference<>();
    Dataset<Integer> dataset = view.observePartitions(
        view.getPartitions(), new PartitionedLogObserver<Integer>() {

          @Override
          public void onRepartition(Collection<Partition> assigned) {
            assertEquals(1, assigned.size());
            try {
              latch.set(new CountDownLatch(1));
              sync.put(1);
            } catch (InterruptedException ex) {
              throw new RuntimeException(ex);
            }
          }

          @Override
          public boolean onNext(
              StreamElement ingest, ConfirmCallback confirm,
              Partition partition, Consumer<Integer> collector) {

            assertEquals(0, partition.getId());
            assertEquals("key", ingest.getKey());
            collector.accept(1);
            confirm.confirm();
            return false;
          }

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }

        });

    MapElements.of(dataset)
        .using(e -> {
          latch.get().countDown();
          return e;
        })
        .output()
        .persist(ListDataSink.get());

    context().getExecutorService().execute(() -> {
      new LocalExecutor().submit(dataset.getFlow()).join();
    });
    // use synchronousqueue as synchronization mechanism only
    assertEquals(1, (int) sync.take());
    writer.online().write(
        StreamElement.update(
            entity, entity.findAttribute("data")
                .orElseThrow(() -> new IllegalStateException(
                    "Missing attribute data")),
            UUID.randomUUID().toString(), "key", "data",
            System.currentTimeMillis(), new byte[] { 1, 2, 3}),
        (succ, exc) -> { });
    latch.get().await();
  }

  private Context context() {
    return new Context(() -> Executors.newCachedThreadPool()) { };
  }

}
