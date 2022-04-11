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
package cz.o2.proxima.direct.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonObject;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.XContentType;

@Slf4j
public class ElasticsearchWriter implements BulkAttributeWriter, BulkProcessor.Listener {
  private final ElasticsearchAccessor accessor;
  private final RestHighLevelClient client;
  private final NavigableMap<Long, CommitCallback> bulkCommits = new ConcurrentSkipListMap<>();
  private final NavigableMap<Long, CommitCallback> awaitingCommits = new ConcurrentSkipListMap<>();

  private CommitCallback lastWrittenOffset;
  private BulkProcessor bulkProcessor;

  public ElasticsearchWriter(ElasticsearchAccessor accessor) {
    this.accessor = accessor;
    this.client = accessor.getRestHighLevelClient();
    this.bulkProcessor =
        BulkProcessor.builder(
                (request, bulkListener) ->
                    client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                this,
                "es-writer-" + accessor.getIndexName())
            .setBulkActions(accessor.getBatchSize())
            .setConcurrentRequests(accessor.getConcurrentRequests())
            .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
            .build();
  }

  @Override
  public URI getUri() {
    return accessor.getUri();
  }

  @Override
  public void rollback() {
    bulkProcessor.close();
    bulkProcessor =
        BulkProcessor.builder(
                (request, bulkListener) ->
                    client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                this)
            .setBulkActions(accessor.getBatchSize())
            .setConcurrentRequests(accessor.getConcurrentRequests())
            .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
            .build();
  }

  @Override
  public synchronized void write(
      StreamElement element, long watermark, CommitCallback commitCallback) {

    if (element.isDelete()) {
      if (element.isDeleteWildcard()) {
        log.warn("Wildcard deletes not supported. Got {}", element);
      } else {
        addDeleteRequest(element, commitCallback);
      }
    } else {
      addIndexRequest(element, commitCallback);
    }
  }

  private void addDeleteRequest(StreamElement element, CommitCallback commitCallback) {
    DeleteRequest request = new DeleteRequest(accessor.getIndexName()).id(toEsKey(element));
    lastWrittenOffset = commitCallback;
    bulkProcessor.add(request);
  }

  private void addIndexRequest(StreamElement element, CommitCallback commitCallback) {
    IndexRequest request =
        new IndexRequest(accessor.getIndexName())
            .id(toEsKey(element))
            .opType(OpType.INDEX)
            .source(toJson(element), XContentType.JSON);
    lastWrittenOffset = commitCallback;
    bulkProcessor.add(request);
  }

  @VisibleForTesting
  String toEsKey(StreamElement element) {
    return element.getKey() + ":" + element.getAttribute();
  }

  @VisibleForTesting
  String toJson(StreamElement element) {
    final JsonObject jsonObject = new JsonObject();

    jsonObject.addProperty("key", element.getKey());
    jsonObject.addProperty("entity", element.getEntityDescriptor().getName());
    jsonObject.addProperty("attribute", element.getAttribute());
    jsonObject.addProperty("timestamp", element.getStamp());
    jsonObject.addProperty("uuid", element.getUuid());
    jsonObject.addProperty("updated_at", System.currentTimeMillis());

    final Optional<Object> data = element.getParsed();
    if (data.isPresent()) {
      @SuppressWarnings("unchecked")
      final AttributeDescriptor<Object> attributeDescriptor =
          (AttributeDescriptor<Object>) element.getAttributeDescriptor();
      final String dataJson = attributeDescriptor.getValueSerializer().asJsonValue(data.get());
      jsonObject.addProperty("data", "${data}");
      return jsonObject.toString().replace("\"${data}\"", dataJson);
    }

    return jsonObject.toString();
  }

  @Override
  public void close() {
    try {
      bulkProcessor.close();
      client.close();
    } catch (IOException e) {
      log.warn("Error closing writer.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void beforeBulk(long executionId, BulkRequest request) {
    log.debug("Bulk starting with executionId: {}", executionId);
    bulkCommits.put(executionId, lastWrittenOffset);
  }

  @Override
  public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
    log.debug("Bulk with executionId: {} finished successfully ", executionId);
    doCommit(executionId, true, null);
  }

  @Override
  public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable failure) {
    log.warn(String.format("Bulk with executionId: %s finished with error", executionId), failure);
    doCommit(executionId, false, failure);
  }

  private void doCommit(long executionId, boolean succ, Throwable err) {
    CommitCallback currentCallback = bulkCommits.remove(executionId);
    if (currentCallback != null) {
      awaitingCommits.put(executionId, currentCallback);
      long uncommittedExecutionId = bulkCommits.isEmpty() ? Long.MAX_VALUE : bulkCommits.firstKey();
      // prevent ConcurrentModificationException
      new ArrayList<>(awaitingCommits.headMap(uncommittedExecutionId).entrySet())
          .forEach(
              e -> {
                awaitingCommits.remove(e.getKey());
                e.getValue().commit(succ, err);
              });
    } else {
      log.warn("Missing commit callback for execution ID {}", executionId);
    }
  }

  @Override
  public Factory<? extends BulkAttributeWriter> asFactory() {
    return repo -> new ElasticsearchWriter(accessor);
  }
}
