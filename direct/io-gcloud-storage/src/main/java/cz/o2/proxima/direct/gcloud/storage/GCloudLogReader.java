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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.blob.BlobLogReader;
import cz.o2.proxima.direct.blob.BlobPath;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.gcloud.storage.GCloudBlobPath.GCloudBlob;
import lombok.extern.slf4j.Slf4j;

/** {@link BatchLogReader} for gcloud storage. */
@Slf4j
public class GCloudLogReader extends BlobLogReader<GCloudBlob, GCloudBlobPath> {

  private final GCloudFileSystem fs;

  public GCloudLogReader(GCloudStorageAccessor accessor, Context context) {
    super(accessor, context);
    this.fs = new GCloudFileSystem(accessor);
  }

  @Override
  protected void runHandlingErrors(GCloudBlob blob, ThrowingRunnable runnable) {
    fs.getRetry()
        .retry(
            () -> {
              try {
                runnable.run();
              } catch (GoogleJsonResponseException ex) {
                if (handleResponseException(ex, blob.getBlob())) {
                  throw new StorageException(ex);
                }
              } catch (Exception ex) {
                handleGeneralException(ex, blob.getBlob());
              }
            });
  }

  @Override
  protected BlobPath<GCloudBlob> createPath(GCloudBlob blob) {
    return new GCloudBlobPath(fs, blob.getBlob());
  }

  private void handleGeneralException(Exception ex, Blob blob) {
    log.warn("Exception while consuming blob {}", blob);
    throw new RuntimeException(ex);
  }

  private boolean handleResponseException(GoogleJsonResponseException ex, Blob blob) {
    switch (ex.getStatusCode()) {
      case 404:
        log.warn(
            "Received 404: {} on getting {}. Skipping gone object.", ex.getStatusMessage(), blob);
        break;
      case 429:
        log.warn("Received 429: {} on getting {}.", ex.getStatusMessage(), blob);
        return true;
      default:
        handleGeneralException(ex, blob);
    }
    return false;
  }

  @Override
  public Factory<?> asFactory() {
    final GCloudStorageAccessor accessor = (GCloudStorageAccessor) getAccessor();
    final Context context = getContext();
    return repo -> new GCloudLogReader(accessor, context);
  }
}
