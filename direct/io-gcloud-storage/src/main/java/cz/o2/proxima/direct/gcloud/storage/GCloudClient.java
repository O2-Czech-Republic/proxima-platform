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
package cz.o2.proxima.direct.gcloud.storage;

import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.UriUtil;
import cz.o2.proxima.util.ExceptionUtils;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class GCloudClient extends AbstractStorage {

  private final Map<String, Object> cfg;
  @Getter private final String bucket;
  @Getter private final String path;
  @Getter private final StorageClass storageClass;
  private final int initialRetryDelay;
  private final int maxRetryDelay;
  @Nullable @Getter private transient Storage client;

  GCloudClient(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    super(entityDesc, uri);
    this.cfg = cfg;
    this.bucket = uri.getAuthority();
    this.path = toPath(uri);
    this.storageClass = getOpt(cfg, "storage-class", StorageClass::valueOf, StorageClass.STANDARD);
    this.initialRetryDelay = getOpt(cfg, "initial-retry-delay-ms", Integer::valueOf, 5000);
    this.maxRetryDelay = getOpt(cfg, "max-retry-delay-ms", Integer::valueOf, (2 << 10) * 5000);
    Preconditions.checkArgument(
        initialRetryDelay < maxRetryDelay / 2,
        "Max retry delay must be at least doble of initial delay, got %s and %s",
        initialRetryDelay,
        maxRetryDelay);
  }

  public Map<String, Object> getCfg() {
    return Collections.unmodifiableMap(cfg);
  }

  // normalize path to not start and to end with slash
  private static String toPath(URI uri) {
    return UriUtil.getPathNormalized(uri) + "/";
  }

  static <T> T getOpt(
      Map<String, Object> cfg, String name, UnaryFunction<String, T> map, T defval) {
    return Optional.ofNullable(cfg.get(name)).map(Object::toString).map(map::apply).orElse(defval);
  }

  void retry(Runnable what) throws StorageException {
    retry(
        () -> {
          what.run();
          return null;
        });
  }

  <T> T retry(Factory<T> what) throws StorageException {
    int delay = initialRetryDelay;
    StorageException caught = null;
    while (true) {
      try {
        return what.apply();
      } catch (StorageException ex) {
        caught = ex;
        boolean shouldRetry = delay <= maxRetryDelay;
        if (shouldRetry) {
          log.warn(
              "Exception while communicating with cloud storage. Retrying after {} ms", delay, ex);
          long effectiveDelay = delay;
          ExceptionUtils.unchecked(() -> TimeUnit.MILLISECONDS.sleep(effectiveDelay));
          delay *= 2;
        } else {
          throw caught;
        }
      }
    }
  }

  Blob createBlob(String name) {
    final String nameNoSlash = dropSlashes(name);
    return retry(
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
