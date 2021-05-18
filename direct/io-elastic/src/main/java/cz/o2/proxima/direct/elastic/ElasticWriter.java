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
package cz.o2.proxima.direct.elastic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;

@Slf4j
public class ElasticWriter implements OnlineAttributeWriter, BulkProcessor.Listener {
  private final ElasticAccessor accessor;
  private final RestHighLevelClient client;
  private final Map<IndexRequest, CommitCallback> callbacksToCommit = new ConcurrentHashMap<>();
  private final BulkProcessor bulkProcessor;

  public ElasticWriter(ElasticAccessor accessor) {
    this.accessor = accessor;
    this.client = accessor.getRestHighLevelClient();
    this.bulkProcessor =
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
  public URI getUri() {
    return accessor.getUri();
  }

  @Override
  public void write(StreamElement element, CommitCallback commitCallback) {
    Preconditions.checkArgument(!element.isDelete(), "Delete not supported.");
    Preconditions.checkArgument(
        !element.getAttributeDescriptor().isWildcard(), "Wildcard not supported.");

    final IndexRequest request =
        new IndexRequest(accessor.getIndexName())
            .id(element.getKey())
            .opType(DocWriteRequest.OpType.INDEX)
            .source(toJson(element), XContentType.JSON);

    callbacksToCommit.put(request, commitCallback);
    bulkProcessor.add(request);
  }

  @VisibleForTesting
  public String toJson(StreamElement element) {
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
      log.warn("Closing problem", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeBulk(long executionId, BulkRequest request) {
    log.debug("Bulk starting with executionId: {}", executionId);
  }

  @Override
  public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
    log.debug("Bulk with executionId: {} finished successfully ", executionId);
    final List<DocWriteRequest<?>> requests = bulkRequest.requests();
    Arrays.stream(bulkResponse.getItems())
        .forEach(
            resp -> {
              final IndexRequest request = (IndexRequest) requests.get(resp.getItemId());
              Preconditions.checkState(
                  request.id().equals(resp.getId()),
                  "Request document id doesn't match with response document id");
              final CommitCallback callback =
                  Objects.requireNonNull(callbacksToCommit.remove(request));
              if (resp.isFailed()) {
                callback.commit(false, resp.getFailure().getCause());
              } else {
                callback.commit(true, null);
              }
            });
  }

  @Override
  public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable failure) {
    log.warn(String.format("Bulk with executionId: %s finished with error", executionId), failure);
    bulkRequest
        .requests()
        .forEach(r -> Objects.requireNonNull(callbacksToCommit.remove(r)).commit(false, failure));
  }

  @Override
  public Factory<? extends OnlineAttributeWriter> asFactory() {
    return repo -> new ElasticWriter(accessor);
  }
}
