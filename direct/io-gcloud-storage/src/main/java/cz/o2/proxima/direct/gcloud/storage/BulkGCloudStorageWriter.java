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

import com.google.cloud.storage.Storage;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.blob.BulkBlobWriter;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.gcloud.storage.GCloudBlobPath.GCloudBlob;
import lombok.extern.slf4j.Slf4j;

/** {@link BulkAttributeWriter} for gcloud storage. */
@Stable
@Slf4j
public class BulkGCloudStorageWriter extends BulkBlobWriter<GCloudBlob, GCloudStorageAccessor> {

  private final GCloudClient client;

  public BulkGCloudStorageWriter(GCloudStorageAccessor accessor, Context context) {
    super(accessor, context);
    this.client = new GCloudClient(accessor.getUri(), accessor.getCfg());
  }

  @Override
  protected void deleteBlobIfExists(GCloudBlob blob) {
    Storage storage = this.client.client();
    if (storage.get(blob.getBlob().getBlobId()).exists()) {
      storage.delete(blob.getName());
    }
  }

  @Override
  public Factory<?> asFactory() {
    final GCloudStorageAccessor accessor = getAccessor();
    final Context context = getContext();
    return repo -> new BulkGCloudStorageWriter(accessor, context);
  }
}
