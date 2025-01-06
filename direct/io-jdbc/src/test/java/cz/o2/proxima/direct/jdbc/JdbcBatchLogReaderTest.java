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
package cz.o2.proxima.direct.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class JdbcBatchLogReaderTest extends JdbcBaseTest {

  @Test(timeout = 20000)
  public void testBatchLogRead() throws InterruptedException {
    assertTrue(
        writeElement(
                accessor,
                StreamElement.upsert(
                    entity,
                    attr,
                    UUID.randomUUID().toString(),
                    "1",
                    attr.getName(),
                    System.currentTimeMillis(),
                    "value".getBytes()))
            .get());
    assertTrue(
        writeElement(
                accessor,
                StreamElement.upsert(
                    entity,
                    attr,
                    UUID.randomUUID().toString(),
                    "2",
                    attr.getName(),
                    System.currentTimeMillis(),
                    "value".getBytes()))
            .get());
    BatchLogReader reader =
        accessor
            .getBatchLogReader(
                repository.getOrCreateOperator(DirectDataOperator.class).getContext())
            .orElseThrow();
    assertEquals(1, reader.getPartitions().size());
    assertEquals(0, reader.getPartitions().get(0).getId());
    List<StreamElement> observed = new ArrayList<>();
    BlockingQueue<Boolean> finished = new SynchronousQueue<>();
    reader.observe(
        reader.getPartitions(),
        Collections.singletonList(attr),
        new BatchLogObserver() {
          @Override
          public boolean onNext(StreamElement element) {
            observed.add(element);
            return true;
          }

          @Override
          public void onCompleted() {
            ExceptionUtils.unchecked(() -> finished.put(true));
          }

          @Override
          public void onCancelled() {
            ExceptionUtils.unchecked(() -> finished.put(false));
          }

          @Override
          public boolean onError(Throwable error) {
            log.warn("Error in observe", error);
            onCancelled();
            return false;
          }
        });
    assertTrue(finished.take());
    assertEquals(2, observed.size());
  }

  @Test
  public void testAsFactory() {
    BatchLogReader reader =
        accessor
            .getBatchLogReader(
                repository.getOrCreateOperator(DirectDataOperator.class).getContext())
            .orElseThrow();
    BatchLogReader clonedReader = reader.asFactory().apply(repository);
    assertEquals(reader, clonedReader);
  }
}
