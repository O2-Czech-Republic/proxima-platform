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

import static cz.o2.proxima.direct.elastic.ElasticAccessor.*;
import static cz.o2.proxima.direct.elastic.ElasticAccessor.DEFAULT_KEYSTORE_TYPE;
import static cz.o2.proxima.direct.elastic.ElasticAccessor.DEFAULT_SOCKET_TIMEOUT_MS;
import static cz.o2.proxima.direct.elastic.ElasticAccessor.parseIndexName;
import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.Repository;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ElasticAccessorTest {

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
  public void testConfigurationDefault() {
    ElasticAccessor accessor =
        new ElasticAccessor(
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
  public void testConfiguration() {
    Map<String, Object> cfg =
        new HashMap<String, Object>() {
          {
            put("elastic.scheme", "https");
            put("elastic.connect-timeout-ms", 10);
            put("elastic.connection-request-timeout-ms", 20);
            put("elastic.socket-timeout-ms", 30);
            put("elastic.keystore-type", "JKS");
            put("elastic.keystore-path", "/opt/k1");
            put("elastic.keystore-password", "secret");
            put("elastic.truststore-path", "/opt/k2");
            put("elastic.truststore-password", "secret2");
          }
        };

    ElasticAccessor accessor =
        new ElasticAccessor(
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
  public void testParseIndexName() {
    assertEquals(
        "my_index", parseIndexName(URI.create("elastic://example.com:9093/my_index/?query=2")));
    assertEquals("my_index", parseIndexName(URI.create("elastic://example.com/my_index")));
    assertEquals("my_index", parseIndexName(URI.create("elastic://example.com/my_index/")));
  }
}
