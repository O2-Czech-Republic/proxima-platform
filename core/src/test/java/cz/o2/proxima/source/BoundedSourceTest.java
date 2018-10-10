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
package cz.o2.proxima.source;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.CountByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.testing.DatasetAssert;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Base class for tests testing bounded sources.
 */
abstract class BoundedSourceTest {

  final Repository repo = ConfigRepository.of(
      ConfigFactory.load("test-reference.conf").resolve());
  final EntityDescriptor entity = getEntity(repo);

  @SuppressWarnings("unchecked")
  final AttributeDescriptor<byte[]> attr = getAttr(entity);

  @SuppressWarnings("unchecked")
  final AttributeDescriptor<byte[]> wildcard = getWildcard(entity);

  /**
   * Get entity to be tested.
   */
  abstract EntityDescriptor getEntity(Repository repo);

  /**
   * Get attribute (non wildcard) to be tested.
   */
  abstract AttributeDescriptor<byte[]> getAttr(EntityDescriptor entity);

  /**
   * Get wildcard attribute to be tested.
   */
  abstract AttributeDescriptor<byte[]> getWildcard(EntityDescriptor entity);


  private StreamElement update(String key, AttributeDescriptor<byte[]> attr) {
    return update(key, attr, System.currentTimeMillis());
  }

  private StreamElement update(
      String key, AttributeDescriptor<byte[]> attr, long stamp) {

    return update(key, attr, attr.getName(), stamp);
  }
  private StreamElement update(
      String key, AttributeDescriptor<byte[]> attr, String attribute, long stamp) {


    return StreamElement.update(entity, attr, UUID.randomUUID().toString(),
        key, attribute, stamp, new byte[] { 1, 2, 3});
  }

  public void testSimpleConsume(
      OnlineAttributeWriter writer, BoundedDataSource<StreamElement> source)
      throws InterruptedException {

    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(update("key", attr, now), (succ, exc) -> latch.countDown());
    latch.await();
    Flow flow = Flow.create();
    Dataset<StreamElement> input = flow.createInput(source);
    ListDataSink<Pair<String, Long>> sink = ListDataSink.get();
    CountByKey.of(input)
        .keyBy(StreamElement::getKey)
        .windowBy(Time.of(Duration.ofHours(1)))
        .output()
        .persist(sink);
    new LocalExecutor().submit(flow).join();
    DatasetAssert.unorderedEquals(
        sink.getOutputs(),
        Pair.of("key", 1L));
  }

  public void testSimpleConsumeWildcard(
      OnlineAttributeWriter writer,
      BoundedDataSource<StreamElement> source) throws InterruptedException {

    long now = System.currentTimeMillis();
    CountDownLatch latch = new CountDownLatch(2);
    writer.write(
        update("key", wildcard, "wildcard.1", now),
        (succ, exc) -> latch.countDown());
    writer.write(
        update("key", wildcard, "wildcard.2", now - 3600000L),
        (succ, exc) -> latch.countDown());
    latch.await();
    Flow flow = Flow.create();
    Dataset<StreamElement> input = flow.createInput(source);
    ListDataSink<Pair<String, Long>> sink = ListDataSink.get();
    input = AssignEventTime.of(input)
        .using(StreamElement::getStamp)
        .output();
    CountByKey.of(input)
        .keyBy(StreamElement::getKey)
        .windowBy(Time.of(Duration.ofHours(1)))
        .output()
        .persist(sink);
    new LocalExecutor().submit(flow).join();
    DatasetAssert.unorderedEquals(
        sink.getOutputs(),
        Pair.of("key", 1L), Pair.of("key", 1L));
  }

  OnlineAttributeWriter getWriter(AttributeDescriptor<byte[]> attr) {
    return repo.getWriter(attr)
        .orElseThrow(() -> new IllegalArgumentException(
            "Attribute " + attr + " has no writer"));
  }

}
