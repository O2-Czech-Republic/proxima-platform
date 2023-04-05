/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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

import static cz.o2.proxima.direct.elasticsearch.ElasticsearchAccessor.*;
import static cz.o2.proxima.direct.elasticsearch.ElasticsearchAccessor.DEFAULT_KEYSTORE_TYPE;
import static cz.o2.proxima.direct.elasticsearch.ElasticsearchAccessor.DEFAULT_SOCKET_TIMEOUT_MS;
import static cz.o2.proxima.direct.elasticsearch.ElasticsearchAccessor.parseIndexName;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ElasticsearchAccessorTest {

  private static final String MODEL =
      "{\n"
          + "  entities: {\n"
          + "    test {\n"
          + "      attributes {\n"
          + "        data: { scheme: \"string\" }\n"
          + "      }\n"
          + "    }\n"
          + "  }\n"
          + "\n"
          + "  attributeFamilies: {\n"
          + "    test_storage_stream {\n"
          + "      entity: test\n"
          + "      attributes: [ data ]\n"
          + "      storage: \"inmem:///test_inmem\"\n"
          + "      type: primary\n"
          + "      access: commit-log\n"
          + "    }\n"
          + "  }\n"
          + "\n"
          + "}\n";

  private final Repository repository = Repository.of(ConfigFactory.parseString(MODEL));

  @Test
  void testConfigurationDefault() {
    ElasticsearchAccessor accessor =
        new ElasticsearchAccessor(
            repository.getEntity("test"),
            URI.create("elastic://example.com/my_index"),
            Collections.emptyMap());
    assertEquals(DEFAULT_SCHEME, accessor.getScheme());
    assertEquals(DEFAULT_CONNECT_TIMEOUT_MS, accessor.getConnectTimeoutMs());
    assertEquals(DEFAULT_CONNECTION_REQUEST_MS, accessor.getConnectionRequestTimeoutMs());
    assertEquals(DEFAULT_SOCKET_TIMEOUT_MS, accessor.getSocketTimeoutMs());
    assertEquals("my_index", accessor.getIndexName());
    assertEquals(DEFAULT_KEYSTORE_TYPE, accessor.getKeystoreType());
    assertEquals("", accessor.getKeystorePassword());
    assertEquals("", accessor.getKeystorePath());
    assertEquals("", accessor.getTruststorePath());
    assertEquals("", accessor.getTruststorePassword());
  }

  @Test
  void testAccessorSerializable() throws IOException, ClassNotFoundException {
    ElasticsearchAccessor accessor =
        new ElasticsearchAccessor(
            repository.getEntity("test"),
            URI.create("elastic://example.com/my_index"),
            Collections.emptyMap());

    ElasticsearchAccessor cloned = TestUtils.assertSerializable(accessor);
    assertEquals(accessor.getUri(), cloned.getUri());
  }

  @Test
  void testConfiguration() {
    Map<String, Object> cfg =
        ImmutableMap.<String, Object>builder()
            .put("elasticsearch.scheme", "https")
            .put("elasticsearch.connect-timeout-ms", 10)
            .put("elasticsearch.connection-request-timeout-ms", 20)
            .put("elasticsearch.socket-timeout-ms", 30)
            .put("elasticsearch.keystore-type", "JKS")
            .put("elasticsearch.keystore-path", "/opt/k1")
            .put("elasticsearch.keystore-password", "secret")
            .put("elasticsearch.truststore-path", "/opt/k2")
            .put("elasticsearch.truststore-password", "secret2")
            .build();

    ElasticsearchAccessor accessor =
        new ElasticsearchAccessor(
            repository.getEntity("test"), URI.create("elastic://example.com/my_index"), cfg);
    assertEquals("https", accessor.getScheme());
    assertEquals(10, accessor.getConnectTimeoutMs());
    assertEquals(20, accessor.getConnectionRequestTimeoutMs());
    assertEquals(30, accessor.getSocketTimeoutMs());
    assertEquals("my_index", accessor.getIndexName());
    assertEquals("JKS", accessor.getKeystoreType());
    assertEquals("/opt/k1", accessor.getKeystorePath());
    assertEquals("secret", accessor.getKeystorePassword());
    assertEquals("/opt/k2", accessor.getTruststorePath());
    assertEquals("secret2", accessor.getTruststorePassword());
  }

  @Test
  void testParseIndexName() {
    assertEquals(
        "my_index", parseIndexName(URI.create("elastic://example.com:9093/my_index/?query=2")));
    assertEquals("my_index", parseIndexName(URI.create("elastic://example.com/my_index")));
    assertEquals("my_index", parseIndexName(URI.create("elastic://example.com/my_index/")));
  }
}
