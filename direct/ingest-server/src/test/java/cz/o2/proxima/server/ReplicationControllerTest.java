/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.server;

import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.AbstractRetryableLogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StreamElement;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link ReplicationController}. */
public class ReplicationControllerTest {

  final Repository repo = Repository.of(() -> ConfigFactory.load("test-reference.conf").resolve());
  final EntityDescriptor event = repo.getEntity("event");
  final AttributeDescriptor<byte[]> data = event.getAttribute("data");
  DirectDataOperator direct;
  ReplicationController controller;
  CompletableFuture<Void> future;
  long now;

  @Before
  public void setUp() {
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
    controller = ReplicationController.of(repo);
    future = controller.runReplicationThreads();
    now = System.currentTimeMillis();
  }

  @After
  public void tearDown() {
    future.cancel(true);
    direct.close();
    repo.discard();
  }

  @Test
  public void testSimpleEventReplication() {
    List<StreamElement> written = new ArrayList<>();
    AbstractRetryableLogObserver observer =
        controller.createOnlineObserver(
            "consumer",
            direct
                .getCommitLogReader(data)
                .orElseThrow(
                    () -> new IllegalArgumentException("Missing commit log reader for data")),
            Sets.newHashSet(data),
            new PassthroughFilter(),
            fakeWriter(written));
    try (ObserveHandle handle = observer.start()) {
      writeEvent();
      assertEquals(1, written.size());
    }
  }

  @Test
  public void testSimpleEventReplicationWithFilter() {
    List<StreamElement> written = new ArrayList<>();
    AbstractRetryableLogObserver observer =
        controller.createOnlineObserver(
            "consumer",
            direct
                .getCommitLogReader(data)
                .orElseThrow(
                    () -> new IllegalArgumentException("Missing commit log reader for data")),
            Sets.newHashSet(data),
            ingest -> false,
            fakeWriter(written));
    try (ObserveHandle handle = observer.start()) {
      writeEvent();
      assertEquals(0, written.size());
    }
  }

  @Test
  public void testEventReplicationWithReadOfInvalidAttribute() {
    List<StreamElement> written = new ArrayList<>();
    EntityDescriptor gateway = repo.getEntity("gateway");
    AttributeDescriptor<byte[]> armed = gateway.getAttribute("armed");
    AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
    AbstractRetryableLogObserver observer =
        controller.createOnlineObserver(
            "consumer",
            direct
                .getCommitLogReader(status)
                .orElseThrow(
                    () -> new IllegalArgumentException("Missing commit log reader for data")),
            Sets.newHashSet(status),
            new PassthroughFilter(),
            fakeWriter(written));
    AtomicInteger commits = new AtomicInteger();
    observer.onNext(
        getUpdate(gateway, armed),
        new OnNextContext() {
          @Override
          public OffsetCommitter committer() {
            return (success, error) -> commits.incrementAndGet();
          }

          @Override
          public Partition getPartition() {
            return null;
          }

          @Override
          public long getWatermark() {
            return 0;
          }

          @Override
          public Offset getOffset() {
            return null;
          }
        });
    assertEquals(1, commits.get());
    assertEquals(0, written.size());
  }

  private OnlineAttributeWriter fakeWriter(List<StreamElement> written) {
    return new OnlineAttributeWriter() {
      @Override
      public void write(StreamElement data, CommitCallback statusCallback) {
        written.add(data);
        statusCallback.commit(true, null);
      }

      @Override
      public URI getUri() {
        return URI.create("fake:///");
      }

      @Override
      public void close() {}
    };
  }

  private void writeEvent() {
    direct
        .getWriter(data)
        .orElseThrow(() -> new IllegalArgumentException("Missing writer for data"))
        .write(getUpdate(event, data), (succ, exc) -> {});
  }

  private StreamElement getUpdate(EntityDescriptor entity, AttributeDescriptor<?> attr) {
    return StreamElement.upsert(
        entity,
        attr,
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        attr.getName(),
        now,
        new byte[] {});
  }
}
