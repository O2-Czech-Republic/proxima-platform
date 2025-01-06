/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.gcloud.storage;

import com.google.cloud.storage.Blob;
import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.direct.io.blob.BlobBase;
import cz.o2.proxima.direct.io.blob.BlobPath;
import cz.o2.proxima.direct.io.bulkfs.FileSystem;
import cz.o2.proxima.direct.io.bulkfs.Path;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects.ToStringHelper;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
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
}
