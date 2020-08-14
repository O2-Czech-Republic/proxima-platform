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

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.blob.BulkBlobWriter;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.s3.S3BlobPath.S3Blob;
import lombok.extern.slf4j.Slf4j;

/** {@link BulkAttributeWriter} for gcloud storage. */
@Stable
@Slf4j
public class BulkS3Writer extends BulkBlobWriter<S3Blob, S3Accessor> {

  private final S3Client client;

  public BulkS3Writer(S3Accessor accessor, Context context) {
    super(accessor, context);
    this.client = new S3Client(accessor.getUri(), accessor.getCfg());
  }

  @Override
  protected void deleteBlobIfExists(S3Blob blob) {
    client.deleteObject(blob.getName());
  }

  @Override
  public Factory<?> asFactory() {
    final S3Accessor accessor = getAccessor();
    final Context context = getContext();
    return repo -> new BulkS3Writer(accessor, context);
  }
}
