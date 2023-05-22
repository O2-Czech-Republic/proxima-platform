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
package cz.o2.proxima.direct.io.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.UploadPartResult;
import cz.o2.proxima.direct.io.s3.S3Client.AmazonS3Factory;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class S3ClientTest {

  @Test
  public void testS3ClientFactory() {
    AmazonS3Factory factory = new AmazonS3Factory(cfg());
    AmazonS3 client = factory.build();
    assertNotNull(client);
  }

  @Test
  public void testUploadLifecycle() throws IOException {
    final AmazonS3 mock = mock(AmazonS3.class);
    final AtomicReference<InitiateMultipartUploadRequest> request = new AtomicReference<>();
    final S3Client client =
        new S3Client(URI.create("s3://bucket/path"), cfg()) {
          @Override
          AmazonS3 client() {
            return mock;
          }
        };
    final String uploadId = UUID.randomUUID().toString();
    when(mock.initiateMultipartUpload(any()))
        .thenAnswer(
            invocationOnMock -> {
              request.set(invocationOnMock.getArgument(0, InitiateMultipartUploadRequest.class));
              final InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
              result.setUploadId(uploadId);
              return result;
            });
    when(mock.uploadPart(any()))
        .thenAnswer(
            invocationOnMock -> {
              final UploadPartResult result = new UploadPartResult();
              result.setETag(UUID.randomUUID().toString());
              return result;
            });
    assertEquals("bucket", client.getBucket());
    try (final OutputStream os = client.putObject("object")) {
      os.write("payload".getBytes(StandardCharsets.UTF_8));
      verify(mock, times(1)).initiateMultipartUpload(any());
      verify(mock, times(0)).uploadPart(any());
      assertEquals("bucket", request.get().getBucketName());
      assertEquals("object", request.get().getKey());
    }
    verify(mock, times(1)).uploadPart(any());
    verify(mock, times(1)).completeMultipartUpload(any());
  }

  @Test
  public void testValidateClientOptions() {
    final URI uri = URI.create("s3://bucket/path");
    Map<String, Object> options = new HashMap<>();
    assertThrows(
        "access-key must not be empty",
        IllegalArgumentException.class,
        () -> new S3Client(uri, options));
    options.put("access-key", "access-key");
    assertThrows(
        "secret-key must not be empty",
        IllegalArgumentException.class,
        () -> new S3Client(uri, options));
    options.put("secret-key", "secret-key");
    verifyCreateS3ClientNotThrowsException(uri, options);

    // validate sse-c options
    options.put("ssec-base64-key", "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0YmVnaXZlbjE=");
    options.put("endpoint", "http://foo.example.com");
    assertThrows(
        "SSL is required when sse-c is enabled.",
        IllegalArgumentException.class,
        () -> new S3Client(uri, options));
    options.put("ssl-enabled", "true");
    verifyCreateS3ClientNotThrowsException(uri, options);
    options.remove("ssl-enabled");
    options.put("endpoint", "https://foo.example.com");
    verifyCreateS3ClientNotThrowsException(uri, options);
  }

  @Test
  public void validateOptionsWithEndpointWithoutScheme() {
    final URI uri = URI.create("s3://bucket/path");
    Map<String, Object> options = new HashMap<>();
    options.put("access-key", "access-key");
    options.put("secret-key", "secret-key");
    options.put("endpoint", "127.0.0.1");
    verifyCreateS3ClientNotThrowsException(uri, options);
    options.put("ssec-base64-key", "MzJieXRlc2xvbmdzZWNyZXRrZXltdXN0YmVnaXZlbjE=");
    assertThrows(
        "SSL is required when sse-c is enabled.",
        IllegalArgumentException.class,
        () -> new S3Client(uri, options));
    options.put("ssl-enabled", "true");
    verifyCreateS3ClientNotThrowsException(uri, options);
    options.remove("ssl-enabled");
    options.put("endpoint", "https://127.0.0.1");
    verifyCreateS3ClientNotThrowsException(uri, options);
  }

  private void verifyCreateS3ClientNotThrowsException(URI uri, Map<String, Object> options) {
    try {
      new S3Client(uri, options);
    } catch (Exception e) {
      log.error("Unexpected exception.", e);
      fail("Unexpected exception " + e.getMessage());
    }
  }

  private Map<String, Object> cfg() {
    return ImmutableMap.<String, Object>builder()
        .put("access-key", "access-key")
        .put("secret-key", "secret-key")
        .put("path-style-access", "true")
        .put("endpoint", "http://endpoint321:123")
        .put("signing-region", "signing-region")
        .put("ssl-enable", "false")
        .put("max-connections", 100)
        .put("connection-timeout-ms", 1000)
        .build();
  }
}
