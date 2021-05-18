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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

@Slf4j
public class ElasticClientFactory {

  @Value
  @Builder
  public static class Configuration {
    @Builder.Default String scheme = "http";
    @Builder.Default String hostnames = "";
    @Builder.Default int connectTimeoutMs = 5_000;
    @Builder.Default int socketTimeoutMs = 30_000;
    @Builder.Default int connectionRequestTimeoutMs = 10_000;
    @Builder.Default String keystoreType = "PKCS12";
    @Builder.Default String keystorePath = "";
    @Builder.Default String keystorePassword = "";
    @Builder.Default String truststorePath = "";
    @Builder.Default String truststorePassword = "";
  }

  public static RestClientBuilder createBuilder(Configuration config) {
    final RestClientBuilder builder =
        RestClient.builder(parseHosts(config.hostnames, config.getScheme()))
            .setRequestConfigCallback(createRequestConfigCallback(config));

    if ("https".equalsIgnoreCase(config.getScheme())) {
      builder.setHttpClientConfigCallback(createConfigurationCallback(config));
    }

    return builder;
  }

  public static RestClient create(Configuration config) {
    return createBuilder(config).build();
  }

  @VisibleForTesting
  public static HttpHost[] parseHosts(String hostnames, String scheme) {
    final List<HttpHost> httpHosts =
        Arrays.stream(hostnames.split(","))
            .map(
                p -> {
                  String[] parts = p.split(":");
                  if (parts.length == 1) {
                    return new HttpHost(parts[0], 9200, scheme);
                  }

                  if (parts.length == 2) {
                    return new HttpHost(parts[0], Integer.parseInt(parts[1]), scheme);
                  }

                  throw new IllegalArgumentException("Invalid host " + p);
                })
            .collect(Collectors.toList());

    final HttpHost[] hostsArray = new HttpHost[httpHosts.size()];
    return httpHosts.toArray(hostsArray);
  }

  private static RestClientBuilder.RequestConfigCallback createRequestConfigCallback(
      Configuration config) {
    return requestConfigBuilder ->
        requestConfigBuilder
            .setConnectTimeout(config.getConnectTimeoutMs())
            .setSocketTimeout(config.getSocketTimeoutMs())
            .setConnectionRequestTimeout(config.getConnectionRequestTimeoutMs());
  }

  private static RestClientBuilder.HttpClientConfigCallback createConfigurationCallback(
      Configuration config) {
    try {
      SSLContextBuilder sslBuilder = SSLContexts.custom();
      loadClientKeyStore(sslBuilder, config);
      loadTrustStore(sslBuilder, config);
      final SSLContext sslContext = sslBuilder.build();
      return httpClientBuilder -> httpClientBuilder.setSSLContext(sslContext);

    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new IllegalArgumentException("Cannot initialize SSLContext", e);
    }
  }

  private static void loadClientKeyStore(SSLContextBuilder sslBuilder, Configuration config) {
    if (config.getKeystorePath().isEmpty()) {
      log.warn("No client keystore configured.");
      return;
    }

    try {
      final String pass =
          config.getKeystorePassword().isEmpty() ? null : config.getKeystorePassword();
      log.info(
          "Using keystore: {}, Password protected: {}", config.getKeystorePath(), pass != null);

      final KeyStore clientKeyStore = createKeyStore(config.getKeystorePath(), pass, config);
      sslBuilder.loadKeyMaterial(clientKeyStore, pass == null ? null : pass.toCharArray());
    } catch (KeyStoreException
        | NoSuchAlgorithmException
        | UnrecoverableKeyException
        | CertificateException e) {

      throw new IllegalArgumentException("Cannot load keystore: " + config.getKeystorePath(), e);
    }
  }

  private static void loadTrustStore(SSLContextBuilder sslBuilder, Configuration config) {
    if (config.getTruststorePath().isEmpty()) {
      log.info("No truststore configured.");
      return;
    }

    try {
      final String pass =
          config.getTruststorePassword().isEmpty() ? null : config.getTruststorePassword();
      log.info(
          "Using truststore: {}, Password protected: {}", config.getTruststorePath(), pass != null);
      final KeyStore clientKeyStore = createKeyStore(config.getTruststorePath(), pass, config);
      sslBuilder.loadTrustMaterial(clientKeyStore, new TrustSelfSignedStrategy());
    } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
      throw new IllegalArgumentException(
          "Cannot load truststore: " + config.getTruststorePath(), e);
    }
  }

  private static KeyStore createKeyStore(
      String keyStorePath, @Nullable String keyStorePassword, Configuration config)
      throws KeyStoreException, CertificateException, NoSuchAlgorithmException {
    final KeyStore keyStore = KeyStore.getInstance(config.getKeystoreType());
    final File keyStoreFile = new File(keyStorePath);
    if (!keyStoreFile.exists()) {
      throw new IllegalArgumentException("Couldn't find file: " + keyStorePath);
    } else {
      char[] keyStorePasswordChars =
          keyStorePassword == null ? null : keyStorePassword.toCharArray();
      try (InputStream is = Files.newInputStream(keyStoreFile.toPath())) {
        keyStore.load(is, keyStorePasswordChars);
      } catch (IOException e) {
        throw new IllegalArgumentException("Couldn't load file: " + keyStorePath);
      }
    }
    return keyStore;
  }
}
