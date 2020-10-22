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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.blob.BlobBase;
import cz.o2.proxima.direct.blob.BlobPath;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.core.Context;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/** A {@link Path} representation of a remote blob in S3. */
@Internal
@ToString
@Slf4j
public class S3BlobPath extends BlobPath<S3BlobPath.S3Blob> implements Path {

  private static final long serialVersionUID = 1L;

  public static class S3Blob implements BlobBase {

    @Getter private final String name;
    private final long size;
    @Nullable private final S3FileSystem fs;

    S3Blob(String name, long size) {
      // For existing files, where we already know the size.
      Preconditions.checkArgument(size >= 0, "Unknown size.");
      this.name = Objects.requireNonNull(name);
      this.size = size;
      this.fs = null;
    }

    @VisibleForTesting
    S3Blob(String name, S3FileSystem fs) {
      // For new files where we don't know the size upfront.
      this.name = Objects.requireNonNull(name);
      this.size = -1;
      this.fs = Objects.requireNonNull(fs);
    }

    @Override
    public long getSize() {
      if (size > -1) {
        return size;
      }
      Objects.requireNonNull(fs);
      try (final S3Object object = fs.getObject(name)) {
        return object.getObjectMetadata().getContentLength();
      } catch (Exception e) {
        log.warn("Unable to retrieve object size of [{}] from [{}].", name, fs.getUri(), e);
        return 0L;
      }
    }
  }

  public static S3BlobPath of(Context context, S3FileSystem fs, String name) {
    return new S3BlobPath(context, fs, new S3Blob(name, fs));
  }

  public static S3BlobPath of(Context context, S3FileSystem fs, String name, long size) {
    return new S3BlobPath(context, fs, new S3Blob(name, size));
  }

  private final Context context;

  @VisibleForTesting
  S3BlobPath(Context context, S3FileSystem fs, S3Blob blob) {
    super(fs, blob);
    this.context = Objects.requireNonNull(context);
  }

  @Override
  public ReadableByteChannel read() {
    final long contentLength = getBlob().getSize();
    return new SeekableByteChannel() {

      private boolean open = true;

      private S3Object object;
      private ReadableByteChannel contentChannel;
      private long position = 0;

      @Override
      public int read(ByteBuffer dst) throws IOException {
        ensureOpen();
        if (position >= contentLength) {
          // End of file.
          return -1;
        }
        if (object == null) {
          final S3FileSystem fs = (S3FileSystem) getFileSystem();
          final GetObjectRequest request = new GetObjectRequest(fs.getBucket(), getBlobName());
          if (position > 0) {
            request.setRange(position, contentLength);
          }
          object = fs.client().getObject(request);
          contentChannel =
              Channels.newChannel(new BufferedInputStream(object.getObjectContent(), 1024 * 1024));
        }

        int totalBytesRead = 0;
        int bytesRead = 0;

        do {
          totalBytesRead += bytesRead;
          try {
            bytesRead = contentChannel.read(dst);
          } catch (AmazonClientException e) {
            throw new IOException(e);
          }
        } while (bytesRead > 0);

        position += totalBytesRead;
        return totalBytesRead;
      }

      @Override
      public long position() throws IOException {
        ensureOpen();
        return position;
      }

      @Override
      public SeekableByteChannel position(long newPosition) throws IOException {
        ensureOpen();
        Preconditions.checkArgument(newPosition >= 0, "New position is too low.");
        Preconditions.checkArgument(newPosition < contentLength, "New position is too high.");

        if (newPosition == position) {
          return this;
        }

        // The position has changed, so close and destroy the object to induce a re-creation on the
        // next call to read()
        if (object != null) {
          object.close();
          object = null;
        }
        position = newPosition;
        return this;
      }

      @Override
      public long size() throws IOException {
        ensureOpen();
        return contentLength;
      }

      @Override
      public boolean isOpen() {
        return open;
      }

      @Override
      public void close() throws IOException {
        if (object != null) {
          object.close();
          object = null;
        }
        open = false;
      }

      @Override
      public int write(ByteBuffer src) {
        throw new NonWritableChannelException();
      }

      @Override
      public SeekableByteChannel truncate(long size) {
        throw new NonWritableChannelException();
      }

      private void ensureOpen() throws IOException {
        if (!open) {
          throw new ClosedChannelException();
        }
      }
    };
  }

  @Override
  public InputStream reader() {
    return Channels.newInputStream(read());
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
