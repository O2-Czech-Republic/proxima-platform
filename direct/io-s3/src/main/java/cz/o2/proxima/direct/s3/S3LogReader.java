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

import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.blob.BlobLogReader;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.s3.S3BlobPath.S3Blob;
import cz.o2.proxima.util.ExceptionUtils;
import lombok.extern.slf4j.Slf4j;

/** {@link BatchLogReader} for gcloud storage. */
@Slf4j
public class S3LogReader extends BlobLogReader<S3Blob, S3BlobPath> {

  private final S3FileSystem fs;
  private final Context context;

  public S3LogReader(S3Accessor accessor, Context context) {
    super(accessor, context);
    this.fs = new S3FileSystem(accessor, context);
    this.context = context;
  }

  @Override
  protected void runHandlingErrors(S3Blob blob, ThrowingRunnable runnable) {
    fs.getRetry().retry(() -> ExceptionUtils.unchecked(runnable::run));
  }

  @Override
  protected S3BlobPath createPath(S3Blob blob) {
    return new S3BlobPath(context, fs, blob);
  }

  @Override
  public Factory<?> asFactory() {
    final S3Accessor accessor = (S3Accessor) getAccessor();
    final Context context = getContext();
    return repo -> new S3LogReader(accessor, context);
  }
}
