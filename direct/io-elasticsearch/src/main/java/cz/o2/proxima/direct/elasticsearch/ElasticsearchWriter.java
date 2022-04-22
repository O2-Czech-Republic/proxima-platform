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
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.Getter;
import lombok.Setter;
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
public class ElasticsearchWriter implements BulkAttributeWriter {

  @VisibleForTesting
  static class BulkProcessorListener implements BulkProcessor.Listener {

    private final NavigableMap<Long, CommitCallback> pendingCommits;
    private final NavigableMap<Long, CommitCallback> confirmedCommits;

    @Setter @Getter CommitCallback lastWrittenOffset;

    BulkProcessorListener(
        NavigableMap<Long, CommitCallback> pendingCommits,
        NavigableMap<Long, CommitCallback> confirmedCommits) {

      this.pendingCommits = pendingCommits;
      this.confirmedCommits = confirmedCommits;
    }

    @Override
    public synchronized void beforeBulk(long executionId, BulkRequest request) {
      log.debug("Bulk starting with executionId: {}", executionId);
      pendingCommits.put(executionId, lastWrittenOffset);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
      log.debug("Bulk with executionId: {} finished successfully ", executionId);
      doCommit(
          executionId,
          !bulkResponse.hasFailures(),
          bulkResponse.hasFailures()
              ? new IllegalStateException(
                  String.format("Failures detected in bulk %s", bulkResponse))
              : null);
    }

    @Override
    public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable failure) {
      log.warn(
          String.format("Bulk with executionId: %s finished with error", executionId), failure);
      doCommit(executionId, false, failure);
    }

    @VisibleForTesting
    void doCommit(long executionId, boolean succ, Throwable err) {
      CommitCallback currentCallback = pendingCommits.remove(executionId);
      if (currentCallback != null) {
        confirmedCommits.put(executionId, currentCallback);
        long uncommittedExecutionId =
            pendingCommits.isEmpty() ? Long.MAX_VALUE : pendingCommits.firstKey();
        // prevent ConcurrentModificationException
        new ArrayList<>(confirmedCommits.headMap(uncommittedExecutionId).entrySet())
            .forEach(
                e -> {
                  confirmedCommits.remove(e.getKey());
                  e.getValue().commit(succ, err);
                });
      } else {
        log.warn("Missing commit callback for execution ID {}", executionId);
      }
    }
  }

  @VisibleForTesting
  interface BulkWriter extends Closeable {
    static BulkWriter viaBulkProcessor(ElasticsearchAccessor accessor) {
      final NavigableMap<Long, CommitCallback> pendingCommits = new ConcurrentSkipListMap<>();
      final NavigableMap<Long, CommitCallback> confirmedCommits = new ConcurrentSkipListMap<>();
      BulkProcessorListener listener = new BulkProcessorListener(pendingCommits, confirmedCommits);
      RestHighLevelClient client = accessor.getRestHighLevelClient();
      BulkProcessor processor =
          BulkProcessor.builder(
                  (request, bulkListener) ->
                      client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                  listener,
                  "es-writer-" + accessor.getIndexName())
              .setBulkActions(accessor.getBatchSize())
              .setConcurrentRequests(accessor.getConcurrentRequests())
              .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
              .build();
      return new BulkWriter() {
        @Override
        public void add(IndexRequest request, CommitCallback commit) {
          synchronized (listener) {
            processor.add(request);
            listener.setLastWrittenOffset(commit);
          }
        }

        @Override
        public void add(DeleteRequest request, CommitCallback commit) {
          synchronized (listener) {
            processor.add(request);
            listener.setLastWrittenOffset(commit);
          }
        }

        @Override
        public void close() throws IOException {
          processor.close();
          client.close();
        }
      };
    }

    void add(IndexRequest request, CommitCallback commit);

    void add(DeleteRequest request, CommitCallback commit);
  }

  private final ElasticsearchAccessor accessor;
  private final DocumentFormatter formatter;
  @VisibleForTesting BulkWriter writer;

  public ElasticsearchWriter(ElasticsearchAccessor accessor) {
    this.accessor = accessor;
    this.formatter = accessor.getDocumentFormatter();
    this.writer = createBulkWriter(accessor);
  }

  @VisibleForTesting
  BulkWriter createBulkWriter(ElasticsearchAccessor accessor) {
    return BulkWriter.viaBulkProcessor(accessor);
  }

  @Override
  public URI getUri() {
    return accessor.getUri();
  }

  @Override
  public void rollback() {
    ExceptionUtils.unchecked(writer::close);
    writer = createBulkWriter(accessor);
  }

  @Override
  public void write(StreamElement element, long watermark, CommitCallback commitCallback) {
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
    writer.add(request, commitCallback);
  }

  private void addIndexRequest(StreamElement element, CommitCallback commitCallback) {
    IndexRequest request =
        new IndexRequest(accessor.getIndexName())
            .id(toEsKey(element))
            .opType(OpType.INDEX)
            .source(toJson(element), XContentType.JSON);
    writer.add(request, commitCallback);
  }

  @VisibleForTesting
  static String toEsKey(StreamElement element) {
    return element.getKey() + ":" + element.getAttribute();
  }

  @VisibleForTesting
  String toJson(StreamElement element) {
    return formatter.toJson(element);
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      log.warn("Error closing writer.", e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Factory<? extends BulkAttributeWriter> asFactory() {
    return repo -> new ElasticsearchWriter(accessor);
  }
}
