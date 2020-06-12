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

import com.amazonaws.services.s3.model.S3Object;
import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.blob.BlobBase;
import cz.o2.proxima.direct.blob.BlobPath;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.core.Context;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/** A {@link Path} representation of a remote blob in S3. */
@Internal
@ToString
@Slf4j
public class S3BlobPath extends BlobPath<S3BlobPath.S3Blob> {

  private static final long serialVersionUID = 1L;

  public static class S3Blob implements BlobBase {

    @Getter private final String name;
    private final S3FileSystem fs;

    @VisibleForTesting
    S3Blob(String name, S3FileSystem fs) {
      this.name = Objects.requireNonNull(name);
      this.fs = fs;
    }

    @Override
    public long getSize() {
      try (final S3Object object = fs.getObject(name)) {
        return object.getObjectMetadata().getContentLength();
      } catch (IOException e) {
        return 0L;
      }
    }
  }

  public static S3BlobPath of(Context context, S3FileSystem fs, String name) {
    return new S3BlobPath(context, fs, new S3Blob(name, fs));
  }

  private final Context context;

  @VisibleForTesting
  S3BlobPath(Context context, S3FileSystem fs, S3Blob blob) {
    super(fs, blob);
    this.context = Objects.requireNonNull(context);
  }

  @Override
  public InputStream reader() {
    return ((S3FileSystem) getFileSystem()).getObject(getBlob().getName()).getObjectContent();
  }

  @Override
  public OutputStream writer() {
    return ((S3Client) getFileSystem()).putObject(getBlobName());
  }

  @Override
  public void delete() {
    ((S3Client) getFileSystem()).deleteObject(getBlob().getName());
  }
}
