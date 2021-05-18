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
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

@Slf4j
public class ElasticAccessor extends AbstractStorage implements DataAccessor {
  private static final long serialVersionUID = 1L;

  private static final String CFG_PREFIX = "elastic.";

  static final String DEFAULT_SCHEME = "http";
  static final int DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
  static final int DEFAULT_CONNECTION_REQUEST_MS = 10_000;
  static final int DEFAULT_SOCKET_TIMEOUT_MS = 60_000;
  static final int DEFAULT_CONCURRENT_REQUESTS = 1;
  static final int DEFAULT_BATCH_SIZE = 100;
  static final String DEFAULT_KEYSTORE_TYPE = "PKCS12";

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

  public ElasticAccessor(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
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
      return Optional.of(new ElasticWriter(this));
    }

    return Optional.empty();
  }

  @Override
  public Optional<CommitLogReader> getCommitLogReader(Context context) {
    return Optional.empty();
  }

  public RestClient getRestClient() {
    return ElasticClientFactory.create(
        new ElasticClientFactory.Configuration(
            getScheme(),
            getUri().getAuthority(),
            getConnectTimeoutMs(),
            getSocketTimeoutMs(),
            getConnectionRequestTimeoutMs(),
            getKeystoreType(),
            getKeystorePath(),
            getKeystorePassword(),
            getTruststorePath(),
            getTruststorePassword()));
  }

  public RestHighLevelClient getRestHighLevelClient() {
    return new RestHighLevelClient(
        ElasticClientFactory.createBuilder(
            new ElasticClientFactory.Configuration(
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
}
