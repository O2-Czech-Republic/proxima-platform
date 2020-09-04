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
package cz.o2.proxima.beam.direct.io;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import java.util.UUID;
import org.junit.Test;

/** Test {@link BlockingQueueLogObserver}. */
public class BlockingQueueLogObserverTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");

  @Test
  public void testWatermarkUpdate() throws InterruptedException {
    testWithStartingWatermark(Long.MIN_VALUE);
    testWithStartingWatermark(0);
  }

  @Test
  public void testMaxWatermarkWhenOnCompleted() {
    BlockingQueueLogObserver observer = BlockingQueueLogObserver.create("name", Long.MIN_VALUE);
    assertEquals(Long.MIN_VALUE, observer.getWatermark());
    observer.onCompleted();
    assertNull(observer.take());
    assertEquals(Long.MAX_VALUE, observer.getWatermark());
  }

  void testWithStartingWatermark(long startingWatermark) throws InterruptedException {
    BlockingQueueLogObserver observer = BlockingQueueLogObserver.create("name", startingWatermark);
    long now = System.currentTimeMillis();
    observer.onNext(newIngest(now), newContext(now));
    assertEquals(startingWatermark, observer.getWatermark());
    StreamElement elem = observer.takeBlocking();
    assertEquals(now, observer.getWatermark());
  }

  private static OnNextContext newContext(long watermark) {
    return new OnNextContext() {
      @Override
      public OffsetCommitter committer() {
        return (succ, exc) -> {};
      }

      @Override
      public Partition getPartition() {
        return Partition.of(0);
      }

      @Override
      public long getWatermark() {
        return watermark;
      }

      @Override
      public Offset getOffset() {
        return null;
      }
    };
  }

  private StreamElement newIngest(long stamp) {
    return StreamElement.upsert(
        gateway,
        status,
        UUID.randomUUID().toString(),
        "key",
        status.getName(),
        stamp,
        new byte[] {1});
  }
}
