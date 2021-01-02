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
package cz.o2.proxima.direct.gcloud.storage;

import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.direct.blob.RetryStrategy;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.storage.UriUtil;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class GCloudClient implements Serializable {

  @Getter private final String bucket;
  @Getter private final String path;
  @Getter private final StorageClass storageClass;
  @Getter private final RetryStrategy retry;
  @Nullable @Getter private transient Storage client;

  GCloudClient(URI uri, Map<String, Object> cfg) {
    this.bucket = uri.getAuthority();
    this.path = toPath(uri);
    this.storageClass = getOpt(cfg, "storage-class", StorageClass::valueOf, StorageClass.STANDARD);
    int initialRetryDelay = getOpt(cfg, "initial-retry-delay-ms", Integer::valueOf, 5000);
    int maxRetryDelay = getOpt(cfg, "max-retry-delay-ms", Integer::valueOf, (2 << 10) * 5000);
    this.retry =
        new RetryStrategy(initialRetryDelay, maxRetryDelay)
            .withRetryableException(StorageException.class);
  }

  // normalize path to not start and to end with slash
  private static String toPath(URI uri) {
    return UriUtil.getPathNormalized(uri) + "/";
  }

  static <T> T getOpt(
      Map<String, Object> cfg, String name, UnaryFunction<String, T> map, T defval) {
    return Optional.ofNullable(cfg.get(name)).map(Object::toString).map(map::apply).orElse(defval);
  }

  Blob createBlob(String name) {
    final String nameNoSlash = dropSlashes(name);
    return retry.retry(
        () ->
            client()
                .create(
                    BlobInfo.newBuilder(bucket, path + nameNoSlash)
                        .setStorageClass(storageClass)
                        .build()));
  }

  private String dropSlashes(String name) {
    String ret = name;
    while (ret.startsWith("/")) {
      ret = ret.substring(1);
    }
    return ret;
  }

  @VisibleForTesting
  Storage client() {
    if (client == null) {
      client =
          StorageOptions.getDefaultInstance()
              .toBuilder()
              .setRetrySettings(ServiceOptions.getDefaultRetrySettings())
              .build()
              .getService();
    }
    return client;
  }
}
