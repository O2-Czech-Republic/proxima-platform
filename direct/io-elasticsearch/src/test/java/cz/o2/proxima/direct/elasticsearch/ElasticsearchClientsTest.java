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

import static cz.o2.proxima.direct.elasticsearch.ElasticsearchClients.parseHosts;
import static java.io.File.createTempFile;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import cz.o2.proxima.direct.elasticsearch.ElasticsearchClients.Configuration;
import java.io.File;
import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.junit.jupiter.api.Test;

class ElasticsearchClientsTest {

  @Test
  void testCreateBuilder() {
    assertNotNull(
        ElasticsearchClients.createBuilder(Configuration.builder().hostnames("host").build()));
    assertNotNull(
        ElasticsearchClients.createBuilder(
            Configuration.builder().scheme("https").hostnames("host").build()));
  }

  @Test
  void testParseHosts() {
    HttpHost[] hosts = parseHosts("example.com:9093,example2.com", "http");
    assertEquals(2, hosts.length);
    assertEquals("example.com", hosts[0].getHostName());
    assertEquals(9093, hosts[0].getPort());
    assertEquals("http", hosts[0].getSchemeName());
    assertEquals("example2.com", hosts[1].getHostName());
    assertEquals(9200, hosts[1].getPort());
    assertEquals("http", hosts[1].getSchemeName());
    assertThrows(
        IllegalArgumentException.class, () -> parseHosts("example.com:9093,invalid:host", "http"));
  }

  @Test
  void testCreateConfigurationCallback() {
    Configuration conf = Configuration.builder().scheme("https").build();
    HttpClientConfigCallback callback = ElasticsearchClients.createConfigurationCallback(conf);
    HttpAsyncClientBuilder builder = mock(HttpAsyncClientBuilder.class);
    callback.customizeHttpClient(builder);
    verify(builder).setSSLContext(any());
  }

  @Test
  void testCreateConfigurationCallbackWithInvalidKeyStore() throws IOException {
    final File p = createTempFile("keystore", ".tmp");
    p.deleteOnExit();
    Configuration conf = Configuration.builder().scheme("https").keystorePath(p.getPath()).build();

    assertThrows(
        IllegalArgumentException.class,
        () -> ElasticsearchClients.createConfigurationCallback(conf));
  }

  @Test
  void testCreateConfigurationCallbackWithInvalidTrustStore() throws IOException {
    final File p = createTempFile("keystore", ".tmp");
    p.deleteOnExit();
    Configuration conf =
        Configuration.builder().scheme("https").truststorePath(p.getPath()).build();

    assertThrows(
        IllegalArgumentException.class,
        () -> ElasticsearchClients.createConfigurationCallback(conf));
  }
}
