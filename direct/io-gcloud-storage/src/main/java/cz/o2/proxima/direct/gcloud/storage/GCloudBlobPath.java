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

import com.google.cloud.storage.Blob;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.blob.BlobBase;
import cz.o2.proxima.direct.blob.BlobPath;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.Path;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import lombok.Getter;

/** A {@link Path} representation of a remote {@link Blob}. */
@Internal
public class GCloudBlobPath extends BlobPath<GCloudBlobPath.GCloudBlob> {

  private static final long serialVersionUID = 1L;

  public static class GCloudBlob implements BlobBase {

    private static final long serialVersionUID = 1L;

    @Getter private final Blob blob;

    @VisibleForTesting
    GCloudBlob(Blob blob) {
      this.blob = blob;
    }

    @Override
    public long getSize() {
      return blob.getSize();
    }

    @Override
    public String getName() {
      return blob.getName();
    }

    @Override
    public String toString() {
      ToStringHelper helper = MoreObjects.toStringHelper(this);
      if (blob != null) {
        return helper.add("blob", blob.getName()).toString();
      }
      return helper.toString();
    }
  }

  static GCloudBlob blob(Blob blob) {
    return new GCloudBlob(blob);
  }

  public static GCloudBlobPath of(FileSystem fs, Blob blob) {
    return new GCloudBlobPath(fs, blob);
  }

  public static GCloudBlobPath of(FileSystem fs, GCloudBlob blob) {
    return new GCloudBlobPath(fs, blob.getBlob());
  }

  @VisibleForTesting
  GCloudBlobPath(FileSystem fs, Blob blob) {
    super(fs, blob(blob));
  }

  @Override
  public InputStream reader() {
    return Channels.newInputStream(getBlob().getBlob().reader());
  }

  @Override
  public OutputStream writer() {
    return Channels.newOutputStream(getBlob().getBlob().writer());
  }

  @Override
  public void delete() {
    Preconditions.checkState(getBlob().getBlob().delete());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("blobName", getBlobName()).toString();
  }
}
