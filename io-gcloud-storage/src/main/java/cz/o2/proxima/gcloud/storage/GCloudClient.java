/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.gcloud.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.URIUtil;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

class GCloudClient extends AbstractStorage {

  @Getter
  final Map<String, Object> cfg;

  @Getter
  final Storage client;

  @Getter
  final String bucket;

  @Getter
  final String path;

  @Getter
  final StorageClass storageClass;

  GCloudClient(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    super(entityDesc, uri);
    this.cfg = cfg;
    this.client = StorageOptions.getDefaultInstance().getService();
    this.bucket = uri.getAuthority();
    this.path = toPath(uri);
    this.storageClass = Optional.ofNullable(cfg.get("storage-class"))
        .map(Object::toString)
        .map(StorageClass::valueOf)
        .orElse(StorageClass.STANDARD);
  }

  // normalize path to not start and to end with slash
  private static String toPath(URI uri) {
    return URIUtil.getPathNormalized() + "/";
  }

  Blob createBlob(String name) {
    return client.create(
        BlobInfo.newBuilder(bucket, path + name)
            .setStorageClass(storageClass)
            .build(),
        Storage.BlobTargetOption.doesNotExist());
  }

}
