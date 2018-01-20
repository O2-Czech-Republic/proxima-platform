/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.gcloud.storage;

import com.google.protobuf.ByteString;
import com.google.protobuf.Parser;
import cz.o2.proxima.gcloud.storage.proto.Serialization;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.seznam.euphoria.shadow.com.google.common.collect.AbstractIterator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Class wrapping serialized elements to a single file with read/write capabilities.
 */
@Slf4j
public class BinaryBlob {

  private static final String MAGIC = "gs::proxima";

  public static class Writer implements Closeable {

    private final boolean gzip;
    private DataOutputStream blobStream = null;

    Writer(boolean gzip, OutputStream out) throws IOException {
      this.gzip = gzip;
      try {
        writeHeader(out);
        blobStream = toOutputStream(out);
      } finally {
        if (blobStream == null && out != null) {
          out.close();
        }
      }
    }

    private void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    private void writeHeader(OutputStream out) throws IOException {
      // don't close this
      DataOutputStream dos = new DataOutputStream(out);
      byte[] header = Serialization.Header.newBuilder()
          .setMagic(MAGIC)
          .setVersion(1)
          .setGzip(gzip)
          .build()
          .toByteArray();
      writeBytes(dos, header);
      dos.flush();
    }

    public void write(StreamElement elem) throws IOException {
      writeBytes(blobStream, toBytes(elem));
    }

    private DataOutputStream toOutputStream(OutputStream out) throws IOException {
      if (gzip) {
        return new DataOutputStream(new GZIPOutputStream(out));
      } else {
        return new DataOutputStream(out);
      }
    }

    private byte[] toBytes(StreamElement data) {
      return Serialization.Element.newBuilder()
          .setKey(data.getKey())
          .setUuid(data.getUuid())
          .setAttribute(data.getAttribute())
          .setDelete(data.isDelete())
          .setDeleteWildcard(data.isDeleteWildcard())
          .setStamp(data.getStamp())
          .setValue(data.getValue() == null
              ? ByteString.EMPTY
              : ByteString.copyFrom(data.getValue()))
          .build()
          .toByteArray();
    }

    @Override
    public void close() throws IOException {
      if (blobStream != null) {
        blobStream.close();
        blobStream = null;
      }
    }

  }

  public static class Reader implements Iterable<StreamElement>, Closeable {

    private final Parser<Serialization.Element> parser = Serialization.Element.parser();
    private final EntityDescriptor entity;
    private final Serialization.Header header;
    private DataInputStream blobStream = null;

    private Reader(EntityDescriptor entity, InputStream in) throws IOException {
      this.entity = entity;
      header = readHeader(in);
      blobStream = toInputStream(in);
    }

    @Override
    public Iterator<StreamElement> iterator() {
      return new AbstractIterator<StreamElement>() {

        @Override
        protected StreamElement computeNext() {
          StreamElement next;
          try {
            next = BinaryBlob.Reader.this.next();
          } catch (EOFException eof) {
            // terminate
            next = null;
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
          if (next != null) {
            return next;
          }
          this.endOfData();
          return null;
        }

      };
    }

    private byte[] readBytes(DataInputStream in) throws IOException {
      byte[] buf = new byte[in.readInt()];
      in.readFully(buf);
      return buf;
    }

    private Serialization.Header readHeader(InputStream in) throws IOException {
      // don't close this
      try {
        DataInputStream dos = new DataInputStream(in);
        Serialization.Header parsed = Serialization.Header.parseFrom(readBytes(dos));
        if (!parsed.getMagic().equals(MAGIC)) {
          throw new IllegalArgumentException(
              "Magic not matching, expected " + MAGIC + " got " + parsed.getMagic());
        }
        return parsed;
      } catch (EOFException eof) {
        log.warn("EOF while reading input. Probably corrupt input?", eof);
        return Serialization.Header.getDefaultInstance();
      }
    }


    private StreamElement next() throws IOException {
      if (blobStream.available() > 0) {
        return fromBytes(readBytes(blobStream));
      }
      return null;
    }

    private DataInputStream toInputStream(InputStream in) throws IOException {
      if (header.getGzip()) {
        return new DataInputStream(new GZIPInputStream(in));
      } else {
        return new DataInputStream(in);
      }
    }

    private StreamElement fromBytes(byte[] data) throws IOException {
      Serialization.Element parsed = parser.parseFrom(data);
      if (parsed.getDelete()) {
        if (parsed.getDeleteWildcard()) {
          return StreamElement.deleteWildcard(entity, getAttr(parsed),
              parsed.getUuid(), parsed.getKey(), parsed.getStamp());
        }
        return StreamElement.delete(entity, getAttr(parsed), parsed.getUuid(),
            parsed.getKey(), parsed.getAttribute(), parsed.getStamp());
      }
      return StreamElement.update(entity, getAttr(parsed), parsed.getUuid(),
          parsed.getKey(), parsed.getAttribute(), parsed.getStamp(),
          parsed.getValue().toByteArray());
    }

    private AttributeDescriptor<?> getAttr(Serialization.Element parsed) {
      return entity.findAttribute(parsed.getAttribute()).orElseThrow(
          () -> new IllegalArgumentException("Unknown attribute " + parsed.getAttribute()));
    }

    @Override
    public void close() throws IOException {
      if (blobStream != null) {
        blobStream.close();
        blobStream = null;
      }
    }

  }

  /**
   * Create writer from given {@link OutputStream}.
   * @param gzip {@code true} if the output be gzipped
   * @param out the {@link OutputStream}
   * @return writer
   * @throws IOException on IO errors
   */
  public static Writer writer(boolean gzip, OutputStream out) throws IOException {
    return new Writer(gzip, out);
  }

  /**
   * Create reader from given entity and {@link InputStream}.
   * @param entity the entity to read attributes for
   * @param in the {@link InputStream}
   * @return reader
   * @throws IOException on IO errors
   */
  public static Reader reader(EntityDescriptor entity, InputStream in)
      throws IOException {
    return new Reader(entity, in);
  }

  @Getter
  private final File path;

  public BinaryBlob(File path) {
    this.path = path;
  }

  public Writer writer(boolean gzip) throws IOException {
    return new Writer(gzip, new FileOutputStream(path));
  }

  public Reader reader(EntityDescriptor entity) throws IOException {
    return new Reader(entity, new FileInputStream(path));
  }

}
