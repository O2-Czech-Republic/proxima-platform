/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.direct.blob.RetryStrategy;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.storage.UriUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class S3Client implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Part size in multi-part upload (5MB). */
  private static final int UPLOAD_PART_SIZE = 5 * 1024 * 1024;

  @VisibleForTesting
  static class AmazonS3Factory {
    private static final Map<String, BiConsumer<Object, AmazonS3ClientBuilder>> UPDATERS =
        new HashMap<>();

    static {
      UPDATERS.put(
          "path-style-access",
          (value, builder) -> builder.setPathStyleAccessEnabled(Boolean.valueOf(value.toString())));
      UPDATERS.put(
          "endpoint",
          (value, builder) ->
              builder.setEndpointConfiguration(
                  new EndpointConfiguration(
                      value.toString(), endpoint(builder).getSigningRegion())));
      UPDATERS.put(
          "signing-region",
          (value, builder) ->
              builder.setEndpointConfiguration(
                  new EndpointConfiguration(
                      endpoint(builder).getServiceEndpoint(), value.toString())));
      UPDATERS.put(
          "ssl-enabled",
          (value, builder) -> {
            if (!Boolean.getBoolean(value.toString())) {
              clientConfiguration(builder).setProtocol(Protocol.HTTP);
            }
          });
      UPDATERS.put("region", (value, builder) -> builder.setRegion(value.toString()));

      UPDATERS.put(
          "max-connections",
          (value, builder) -> clientConfiguration(builder).setMaxConnections((int) value));

      UPDATERS.put(
          "connection-timeout-ms",
          (value, builder) -> clientConfiguration(builder).setConnectionTimeout((int) value));
    }

    private static ClientConfiguration clientConfiguration(AmazonS3ClientBuilder builder) {
      return Optional.ofNullable(builder.getClientConfiguration())
          .orElse(new ClientConfiguration());
    }

    static EndpointConfiguration endpoint(AmazonS3ClientBuilder builder) {
      return Optional.ofNullable(builder.getEndpoint()).orElse(new EndpointConfiguration("", ""));
    }

    private final Map<String, Object> cfg;

    AmazonS3Factory(Map<String, Object> cfg) {
      this.cfg = cfg;
    }

    AmazonS3 build() {
      validate();
      AmazonS3ClientBuilder builder = AmazonS3Client.builder();
      UPDATERS.forEach(
          (name, updater) ->
              Optional.ofNullable(cfg.get(name))
                  .ifPresent(value -> updater.accept(value, builder)));
      String accessKey = getOpt(cfg, "access-key", Object::toString, "");
      String secretKey = getOpt(cfg, "secret-key", Object::toString, "");
      builder.setCredentials(
          new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
              return new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                  return accessKey;
                }

                @Override
                public String getAWSSecretKey() {
                  return secretKey;
                }
              };
            }

            @Override
            public void refresh() {}
          });
      return builder.build();
    }

    private void validate() {
      String accessKey = getOpt(cfg, "access-key", Object::toString, "");
      String secretKey = getOpt(cfg, "secret-key", Object::toString, "");
      Preconditions.checkArgument(!accessKey.isEmpty(), "access-key must not be empty");
      Preconditions.checkArgument(!secretKey.isEmpty(), "secret-key must not be empty");
    }
  }

  @Getter private final String bucket;
  @Getter private final String path;
  @Getter private final RetryStrategy retry;
  private final Map<String, Object> cfg;
  @Nullable private transient AmazonS3 client;

  S3Client(URI uri, Map<String, Object> cfg) {
    this.bucket = uri.getAuthority();
    this.path = toPath(uri);
    int initialRetryDelay = getOpt(cfg, "initial-retry-delay-ms", Integer::valueOf, 5000);
    int maxRetryDelay = getOpt(cfg, "max-retry-delay-ms", Integer::valueOf, (2 << 10) * 5000);
    this.retry = new RetryStrategy(initialRetryDelay, maxRetryDelay);
    this.cfg = cfg;
    new AmazonS3Factory(cfg).validate();
  }

  // normalize path to not start and to end with slash
  private static String toPath(URI uri) {
    return UriUtil.getPathNormalized(uri) + "/";
  }

  static <T> T getOpt(
      Map<String, Object> cfg, String name, UnaryFunction<String, T> map, T defval) {
    return Optional.ofNullable(cfg.get(name)).map(Object::toString).map(map::apply).orElse(defval);
  }

  @VisibleForTesting
  AmazonS3 client() {
    if (client == null) {
      client = new AmazonS3Factory(cfg).build();
    }
    return client;
  }

  public S3Object getObject(String blobName) {
    return client().getObject(getBucket(), blobName);
  }

  public void deleteObject(String key) {
    client().deleteObject(getBucket(), key);
  }

  /**
   * Put object to s3 using multi-part upload.
   *
   * @param blobName Name of the blob we want to write.
   * @return Output stream that we can write data into.
   */
  public OutputStream putObject(String blobName) {
    Preconditions.checkState(!client().doesObjectExist(bucket, blobName), "Object already exists.");
    final String currentBucket = getBucket();
    final String uploadId =
        client()
            .initiateMultipartUpload(new InitiateMultipartUploadRequest(currentBucket, blobName))
            .getUploadId();
    final List<PartETag> eTags = new ArrayList<>();
    final byte[] partBuffer = new byte[UPLOAD_PART_SIZE];
    return new OutputStream() {

      /** Signalizes whether this output stream is closed. */
      private boolean closed = false;

      /** Number of un-flushed bytes in current part buffer. */
      private int currentBytes = 0;

      /** Part number of current part in multi-part upload. Indexing from 1. */
      private int partNumber = 1;

      @Override
      public void write(int b) throws IOException {
        Preconditions.checkState(!closed, "Output stream already closed.");
        // Number of bytes written is also position of next write.
        partBuffer[currentBytes] = (byte) b;
        currentBytes++;
        if (currentBytes >= UPLOAD_PART_SIZE) {
          flush();
        }
      }

      @Override
      public void flush() throws IOException {
        Preconditions.checkState(!closed, "Output stream already closed.");
        if (currentBytes > 0) {
          try (final InputStream is = new ByteArrayInputStream(partBuffer, 0, currentBytes)) {
            final UploadPartRequest uploadPartRequest =
                new UploadPartRequest()
                    .withBucketName(currentBucket)
                    .withKey(blobName)
                    .withUploadId(uploadId)
                    .withPartNumber(partNumber)
                    .withInputStream(is)
                    .withPartSize(currentBytes);
            final UploadPartResult uploadPartResult = client().uploadPart(uploadPartRequest);
            eTags.add(uploadPartResult.getPartETag());
            partNumber++;
          }
        }
        currentBytes = 0;
      }

      @Override
      public void close() throws IOException {
        if (!closed) {
          flush();
          client()
              .completeMultipartUpload(
                  new CompleteMultipartUploadRequest(currentBucket, blobName, uploadId, eTags));
          closed = true;
        }
      }
    };
  }
}
