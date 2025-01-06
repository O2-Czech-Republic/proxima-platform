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
package cz.o2.proxima.direct.io.elasticsearch;

import static org.junit.jupiter.api.Assertions.*;

import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.core.util.TestUtils;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.io.elasticsearch.ElasticsearchWriter.BulkProcessorListener;
import cz.o2.proxima.direct.io.elasticsearch.ElasticsearchWriter.BulkWriter;
import cz.o2.proxima.elasticsearch.shaded.org.elasticsearch.action.DocWriteRequest.OpType;
import cz.o2.proxima.elasticsearch.shaded.org.elasticsearch.action.bulk.BulkItemResponse;
import cz.o2.proxima.elasticsearch.shaded.org.elasticsearch.action.bulk.BulkResponse;
import cz.o2.proxima.elasticsearch.shaded.org.elasticsearch.action.delete.DeleteRequest;
import cz.o2.proxima.elasticsearch.shaded.org.elasticsearch.action.index.IndexRequest;
import cz.o2.proxima.internal.com.google.gson.JsonObject;
import cz.o2.proxima.internal.com.google.gson.JsonParser;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class ElasticsearchWriterTest {

  private final Repository repo = Repository.ofTest(ConfigFactory.load("test-es.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Regular<Float> metric = Regular.of(gateway, gateway.getAttribute("metric"));

  @Test
  void testAsFactorySerializable() {
    ElasticsearchStorage storage = new ElasticsearchStorage();
    ElasticsearchAccessor accessor =
        storage.createAccessor(direct, repo.getFamilyByName("gateway-to-es"));
    ElasticsearchWriter writer =
        (ElasticsearchWriter) Optionals.get(accessor.getWriter(direct.getContext()));
    assertDoesNotThrow(
        () -> TestUtils.deserializeObject(TestUtils.serializeObject(writer.asFactory())));
    assertEquals(writer.asFactory().apply(repo).getUri(), writer.getUri());
  }

  @Test
  void testWriterSimple() {
    ElasticsearchStorage storage = new ElasticsearchStorage();
    ElasticsearchAccessor accessor =
        storage.createAccessor(direct, repo.getFamilyByName("gateway-to-es"));
    ElasticsearchWriter writer =
        (ElasticsearchWriter) Optionals.get(accessor.getWriter(direct.getContext()));
    StreamElement element = metric.upsert("key", System.currentTimeMillis(), 1.0f);
    String json = writer.toJson(element);
    JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
    assertEquals("key", obj.get("key").getAsString());
    assertEquals("gateway", obj.get("entity").getAsString());
    assertEquals("metric", obj.get("attribute").getAsString());
    assertTrue(obj.has("timestamp"));
    assertTrue(obj.has("updated_at"));
    assertTrue(obj.has("uuid"));
    assertEquals(1.0f, obj.get("data").getAsFloat(), 0.0001);
    assertEquals("key:metric", writer.toEsKey(element));
    assertEquals(accessor.getUri(), writer.getUri());

    BulkWriter oldWriter = writer.writer;
    writer.rollback();
    assertNotSame(oldWriter, writer.writer);

    element = metric.delete("key", System.currentTimeMillis());
    json = writer.toJson(element);
    obj = JsonParser.parseString(json).getAsJsonObject();
    assertEquals("key", obj.get("key").getAsString());
    assertEquals("gateway", obj.get("entity").getAsString());
    assertEquals("metric", obj.get("attribute").getAsString());
    assertTrue(obj.has("timestamp"));
    assertTrue(obj.has("updated_at"));
    assertTrue(obj.has("uuid"));
    assertEquals("key:metric", writer.toEsKey(element));
  }

  @Test
  @Timeout(5)
  void testBulkWrite() throws InterruptedException {
    ElasticsearchStorage storage = new ElasticsearchStorage();
    ElasticsearchAccessor accessor =
        storage.createAccessor(direct, repo.getFamilyByName("gateway-to-es"));
    List<IndexRequest> indexRequestList = new ArrayList<>();
    ElasticsearchWriter writer =
        new ElasticsearchWriter(accessor) {
          @Override
          BulkWriter createBulkWriter(ElasticsearchAccessor accessor) {
            return new BulkWriter() {
              @Override
              public void add(IndexRequest request, CommitCallback commit) {
                indexRequestList.add(request);
                commit.commit(true, null);
              }

              @Override
              public void add(DeleteRequest request, CommitCallback commit) {}

              @Override
              public void close() {}
            };
          }
        };
    long now = System.currentTimeMillis();
    StreamElement element = metric.upsert("key", now, 1.0f);
    BlockingQueue<Pair<Boolean, Throwable>> res = new ArrayBlockingQueue<>(1);
    writer.write(
        element, now, (succ, exc) -> ExceptionUtils.unchecked(() -> res.put(Pair.of(succ, exc))));
    Pair<Boolean, Throwable> taken = res.take();
    assertTrue(taken.getFirst());
    assertNull(taken.getSecond());
    assertEquals(1, indexRequestList.size());
    JsonObject json =
        JsonParser.parseString(indexRequestList.get(0).source().utf8ToString()).getAsJsonObject();
    assertEquals(1.0f, json.get("data").getAsFloat(), 0.001);
    assertEquals("my_index", indexRequestList.get(0).index());
    assertEquals("key:metric", indexRequestList.get(0).id());
  }

  @Test
  @Timeout(5)
  void testBulkDelete() throws InterruptedException {
    ElasticsearchStorage storage = new ElasticsearchStorage();
    ElasticsearchAccessor accessor =
        storage.createAccessor(direct, repo.getFamilyByName("gateway-to-es"));
    List<DeleteRequest> deleteRequestList = new ArrayList<>();
    ElasticsearchWriter writer =
        new ElasticsearchWriter(accessor) {
          @Override
          BulkWriter createBulkWriter(ElasticsearchAccessor accessor) {
            return new BulkWriter() {
              @Override
              public void add(IndexRequest request, CommitCallback commit) {}

              @Override
              public void add(DeleteRequest request, CommitCallback commit) {
                deleteRequestList.add(request);
                commit.commit(true, null);
              }

              @Override
              public void close() {}
            };
          }
        };
    long now = System.currentTimeMillis();
    StreamElement element = metric.delete("key", now);
    BlockingQueue<Pair<Boolean, Throwable>> res = new ArrayBlockingQueue<>(1);
    writer.write(
        element, now, (succ, exc) -> ExceptionUtils.unchecked(() -> res.put(Pair.of(succ, exc))));
    Pair<Boolean, Throwable> taken = res.take();
    assertTrue(taken.getFirst());
    assertNull(taken.getSecond());
    assertEquals(1, deleteRequestList.size());
    assertEquals("my_index", deleteRequestList.get(0).index());
    assertEquals("key:metric", deleteRequestList.get(0).id());
  }

  @Test
  void testBulkProcessorListener() {
    NavigableMap<Long, CommitCallback> pendingCommits = new TreeMap<>();
    NavigableMap<Long, CommitCallback> confirmedCommits = new TreeMap<>();
    BulkProcessorListener listener = new BulkProcessorListener(pendingCommits, confirmedCommits);
    AtomicReference<Pair<Boolean, Integer>> commit = new AtomicReference<>();
    listener.setLastWrittenOffset(committed(commit, 1));
    listener.beforeBulk(1, null);
    listener.afterBulk(1, null, okResponse());
    assertEquals(1, commit.get().getSecond());
    assertTrue(commit.get().getFirst());

    listener.setLastWrittenOffset(committed(commit, 2));
    listener.beforeBulk(2, null);
    listener.setLastWrittenOffset(committed(commit, 3));
    listener.beforeBulk(3, null);
    listener.afterBulk(3, null, okResponse());
    assertEquals(1, commit.get().getSecond());
    listener.afterBulk(2, null, okResponse());
    assertEquals(3, commit.get().getSecond());

    listener.setLastWrittenOffset(committed(commit, 4));
    listener.beforeBulk(4, null);
    listener.afterBulk(4, null, new RuntimeException("ex"));
    assertEquals(4, commit.get().getSecond());
    assertFalse(commit.get().getFirst());

    listener.setLastWrittenOffset(committed(commit, 5));
    listener.beforeBulk(5, null);
    listener.afterBulk(5, null, failedResponse());
    assertEquals(5, commit.get().getSecond());
    assertFalse(commit.get().getFirst());

    // must be ignored, there is no 'beforeBulk'
    listener.afterBulk(6, null, okResponse());
    assertEquals(5, commit.get().getSecond());
  }

  private BulkResponse okResponse() {
    return new BulkResponse(new BulkItemResponse[] {}, 0);
  }

  private BulkResponse failedResponse() {
    return new BulkResponse(
        new BulkItemResponse[] {
          BulkItemResponse.failure(1, OpType.INDEX, new BulkItemResponse.Failure("", "", "", null))
        },
        0);
  }

  private CommitCallback committed(AtomicReference<Pair<Boolean, Integer>> commit, int value) {
    return (succ, exc) -> commit.set(Pair.of(succ, value));
  }
}
