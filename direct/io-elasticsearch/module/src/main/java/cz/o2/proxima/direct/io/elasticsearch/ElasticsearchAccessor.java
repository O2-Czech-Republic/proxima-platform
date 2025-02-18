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

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage.SerializableAbstractStorage;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.elasticsearch.shaded.org.elasticsearch.client.RestHighLevelClient;
import cz.o2.proxima.elasticsearch.shaded.org.elasticsearch.core.TimeValue;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticsearchAccessor extends SerializableAbstractStorage implements DataAccessor {

  private static final long serialVersionUID = 1L;

  private static final String CFG_PREFIX = "elasticsearch.";

  static final String DEFAULT_SCHEME = "http";
  static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
  static final int DEFAULT_CONNECTION_REQUEST_MS = 10_000;
  static final int DEFAULT_SOCKET_TIMEOUT_MS = 60_000;
  static final int DEFAULT_CONCURRENT_REQUESTS = 1;
  static final int DEFAULT_BATCH_SIZE = 100;
  static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";
  static final int DEFAULT_BULK_SIZE_MB = 10;

  @Getter private final Map<String, Object> cfg;
  @Getter private final String scheme;
  @Getter private final String indexName;
  @Getter private final int connectTimeoutMs;
  @Getter private final int connectionRequestTimeoutMs;
  @Getter private final int socketTimeoutMs;
  @Getter private final int concurrentRequests;
  @Getter private final int batchSize;
  @Getter private final String keystoreType;
  @Getter private final String keystorePath;
  @Getter private final String keystorePassword;
  @Getter private final String truststorePath;
  @Getter private final String truststorePassword;
  @Getter private final DocumentFormatter documentFormatter;
  private final int flushIntervalMs;
  @Getter private final int bulkSizeMb;

  public ElasticsearchAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    super(entityDesc, uri);
    this.cfg = cfg;
    this.scheme = getStringConfig("scheme", DEFAULT_SCHEME);
    this.connectTimeoutMs = getIntConfig("connect-timeout-ms", DEFAULT_CONNECT_TIMEOUT_MS);
    this.connectionRequestTimeoutMs =
        getIntConfig("connection-request-timeout-ms", DEFAULT_CONNECTION_REQUEST_MS);
    this.socketTimeoutMs = getIntConfig("socket-timeout-ms", DEFAULT_SOCKET_TIMEOUT_MS);
    this.concurrentRequests =
        getIntConfig("concurrent-batch-requests", DEFAULT_CONCURRENT_REQUESTS);
    this.batchSize = getIntConfig("batch-size", DEFAULT_BATCH_SIZE);
    this.keystoreType = getStringConfig("keystore-type", DEFAULT_KEYSTORE_TYPE);
    this.keystorePath = getStringConfig("keystore-path");
    this.keystorePassword = getStringConfig("keystore-password");
    this.truststorePath = getStringConfig("truststore-path");
    this.truststorePassword = getStringConfig("truststore-password");
    this.indexName = parseIndexName(uri);
    this.documentFormatter =
        Classpath.newInstance(
            getStringConfig("document-formatter", DocumentFormatter.Default.class.getName()),
            DocumentFormatter.class);
    this.flushIntervalMs = getIntConfig("flush-interval-ms", 0);
    this.bulkSizeMb = getIntConfig("bulk-size-mb", DEFAULT_BULK_SIZE_MB);
  }

  @VisibleForTesting
  public static String parseIndexName(URI uri) {
    String path = uri.getPath();
    while (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    if (path.length() <= 1) {
      throw new IllegalArgumentException(
          "Invalid path in elastic URI " + uri + ". The path represents name of index");
    }
    return path.substring(1);
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    if (getUri().getScheme().startsWith("elastic")) {
      return Optional.of(new ElasticsearchWriter(this));
    }

    return Optional.empty();
  }

  @Override
  public Optional<CommitLogReader> getCommitLogReader(Context context) {
    return Optional.empty();
  }

  public RestHighLevelClient getRestHighLevelClient() {
    return new RestHighLevelClient(
        ElasticsearchClients.createBuilder(
            new ElasticsearchClients.Configuration(
                getScheme(),
                getUri().getAuthority(),
                getConnectTimeoutMs(),
                getSocketTimeoutMs(),
                getConnectionRequestTimeoutMs(),
                getKeystoreType(),
                getKeystorePath(),
                getKeystorePassword(),
                getTruststorePath(),
                getTruststorePassword())));
  }

  private int getIntConfig(String key, int defaultValue) {
    return Integer.parseInt(cfg.getOrDefault(CFG_PREFIX + key, defaultValue).toString());
  }

  private String getStringConfig(String key) {
    return getStringConfig(key, "");
  }

  private String getStringConfig(String key, String defaultValue) {
    return cfg.getOrDefault(CFG_PREFIX + key, defaultValue).toString();
  }

  public @Nullable TimeValue getFlushInterval() {
    return flushIntervalMs > 0 ? TimeValue.timeValueMillis(flushIntervalMs) : null;
  }
}
