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
package cz.o2.proxima.example.model;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.example.event.Event.BaseEvent;
import cz.o2.proxima.example.user.User.Details;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

public class ModelTest {

  private Model model = Model.of(ConfigFactory.load().resolve());

  @Test
  public void testSimplifiedUpsertFromGeneratedSource() {
    StreamElement element =
        model
            .getUser()
            .getDetailsDescriptor()
            .upsert("key", Instant.now(), Details.newBuilder().setEmail("email").build());
    assertNotNull(element);
  }

  @Test
  public void testSimplifiedUpsertWildcard() {
    StreamElement element =
        model
            .getUser()
            .getEventDescriptor()
            .upsert("user", "key", "suffix", Instant.now(), BaseEvent.newBuilder().build());
    assertEquals("key", element.getKey());
  }

  @Test
  public void testReadWrite() throws InterruptedException {
    Regular<BaseEvent> desc = model.getEvent().getDataDescriptor();
    Optional<OnlineAttributeWriter> writer = model.directOperator().getWriter(desc);
    writer
        .get()
        .write(
            desc.upsert(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                BaseEvent.newBuilder().setUserName("user").build()),
            (succ, exc) -> {});
    Optional<CommitLogReader> reader = model.directOperator().getCommitLogReader(desc);
    List<String> users = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    reader
        .get()
        .observeBulk(
            "consumer",
            Position.OLDEST,
            true,
            new LogObserver() {

              @Override
              public boolean onError(Throwable error) {
                return false;
              }

              @Override
              public void onCompleted() {
                latch.countDown();
              }

              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                Optional<BaseEvent> baseEvent = desc.valueOf(ingest);
                users.add(baseEvent.get().getUserName());
                return true;
              }
            });
    latch.await();
    assertEquals("user", users.get(0));
  }
}
