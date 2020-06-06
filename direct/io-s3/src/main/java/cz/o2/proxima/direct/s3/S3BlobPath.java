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
import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.blob.BlobBase;
import cz.o2.proxima.direct.blob.BlobPath;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/** A {@link Path} representation of a remote blob in S3. */
@Internal
@ToString
@Slf4j
public class S3BlobPath extends BlobPath<S3BlobPath.S3Blob> {

  private static final long serialVersionUID = 1L;

  @VisibleForTesting
  static class CopyInputOutputStream {

    private static class Block {
      private static final int SIZE = 1024;
      final byte[] buf = new byte[SIZE];
      int writePos = 0;

      boolean add(int i) {
        buf[writePos++] = (byte) i;
        return writePos >= SIZE;
      }

      boolean isEmpty() {
        return writePos == 0;
      }

      public int size() {
        return writePos;
      }
    }

    private final List<Block> written = Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch writeLatch;
    private Block current = new Block();
    AtomicBoolean finished = new AtomicBoolean();
    AtomicReference<Throwable> error = new AtomicReference<>();

    CopyInputOutputStream() {
      this(0);
    }

    CopyInputOutputStream(int waitForConsumers) {
      writeLatch = new CountDownLatch(waitForConsumers);
    }

    public InputStream read() {
      return new InputStream() {
        int readBlock = 0;
        int readPosition = 0;
        Block reading = null;

        @Override
        public int read() {
          while (!finished.get() || readBlock < written.size()) {
            if (reading == null) {
              if (readBlock < written.size()) {
                reading = written.get(readBlock++);
                readPosition = 0;
              } else {
                synchronized (written) {
                  ExceptionUtils.unchecked(() -> written.wait(50));
                }
                continue;
              }
            }
            if (readPosition < reading.size()) {
              return reading.buf[readPosition++] & 0xFF;
            }
            reading = null;
          }
          return -1;
        }
      };
    }

    public OutputStream write() {
      return new OutputStream() {
        @Override
        public void write(int i) {
          synchronized (written) {
            if (!current.add(i)) {
              written.add(current);
              current = new Block();
            }
            written.notifyAll();
          }
        }

        @Override
        public void close() throws IOException {
          if (!current.isEmpty()) {
            written.add(current);
            current = new Block();
          }
          finished.set(true);
          ExceptionUtils.unchecked(writeLatch::await);
          Throwable errorCaught = error.getAndSet(null);
          if (errorCaught != null) {
            throw new IOException(errorCaught);
          }
        }
      };
    }

    void setError(Throwable err) {
      error.set(err);
      consumed();
    }

    public void consumed() {
      writeLatch.countDown();
    }
  }

  public static class S3Blob implements BlobBase {

    @Getter private final String name;

    @Getter(AccessLevel.PRIVATE)
    private final @Nullable S3Object remoteObject;

    @VisibleForTesting
    S3Blob(String name) {
      this.name = Objects.requireNonNull(name);
      this.remoteObject = null;
    }

    @Override
    public long getSize() {
      return Optional.ofNullable(remoteObject)
          .map(o -> o.getObjectMetadata().getContentLength())
          .orElse(0L);
    }
  }

  public static S3BlobPath of(Context context, FileSystem fs, String name) {
    return new S3BlobPath(context, fs, new S3Blob(name));
  }

  private final Context context;

  @VisibleForTesting
  S3BlobPath(Context context, FileSystem fs, S3Blob blob) {
    super(fs, blob);
    this.context = Objects.requireNonNull(context);
  }

  @Override
  public InputStream reader() {
    Preconditions.checkState(
        getBlob().getRemoteObject() != null,
        "Cannot read from not-yet written object [%s]",
        getBlobName());
    return getBlob().getRemoteObject().getObjectContent();
  }

  @Override
  public OutputStream writer() {
    Preconditions.checkState(
        getBlob().getRemoteObject() == null,
        "Cannot write to already put object [%s]",
        getBlob().getName());
    CopyInputOutputStream copy = new CopyInputOutputStream(1);
    OutputStream ret = copy.write();
    context
        .getExecutorService()
        .submit(
            () -> {
              try (InputStream input = copy.read()) {
                ((S3Client) getFileSystem()).putObject(getBlobName(), input);
                copy.consumed();
              } catch (Throwable err) {
                log.error("Failed to put object {}", getBlobName(), err);
                copy.setError(err);
              }
            });
    return ret;
  }

  @Override
  public void delete() {
    if (getBlob().getRemoteObject() != null) {
      ((S3Client) getFileSystem()).deleteObject(getBlob().getRemoteObject().getKey());
    }
  }
}
