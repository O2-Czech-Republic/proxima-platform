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
package cz.o2.proxima.direct.blob;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.Path;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.ToString;

/** A {@link Path} representation of a remote blob. */
@Internal
@ToString
public abstract class BlobPath<BlobT extends BlobBase> implements Path {

  private static final long serialVersionUID = 1L;

  private final FileSystem fs;
  private final BlobT blob;

  public BlobPath(FileSystem fs, BlobT blob) {
    this.fs = fs;
    this.blob = Objects.requireNonNull(blob);
  }

  @Override
  public abstract InputStream reader();

  @Override
  public abstract OutputStream writer();

  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public abstract void delete();

  public BlobT getBlob() {
    return blob;
  }

  public String getBlobName() {
    return blob.getName();
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(@Nonnull Path other) {
    return getBlobName().compareTo(((BlobPath<BlobT>) other).getBlobName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getFileSystem().getUri(), getBlobName().hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof BlobPath)) {
      return false;
    }
    BlobPath<?> other = (BlobPath<?>) obj;
    return other.getFileSystem().getUri().equals(getFileSystem().getUri())
        && other.getBlobName().equals(getBlobName());
  }

  @VisibleForTesting
  public static String normalizePath(String path) {
    StringBuilder sb = new StringBuilder();
    boolean lastSlash = true;
    for (char ch : path.toCharArray()) {
      if (ch == '/') {
        if (!lastSlash) {
          lastSlash = true;
          sb.append(ch);
        }
      } else {
        lastSlash = false;
        sb.append(ch);
      }
    }
    return sb.toString();
  }
}
