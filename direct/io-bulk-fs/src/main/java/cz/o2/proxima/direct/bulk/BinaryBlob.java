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
package cz.o2.proxima.direct.bulk;

import com.google.common.collect.AbstractIterator;
import com.google.protobuf.ByteString;
import com.google.protobuf.Parser;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.gcloud.storage.proto.Serialization;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Class wrapping serialized elements to a single file with read/write capabilities. */
@Slf4j
@Internal
public class BinaryBlob {

  private static final String MAGIC = "gs::proxima";

  public static class BinaryBlobWriter implements Writer {

    private final Path path;
    private final boolean gzip;
    private DataOutputStream blobStream = null;

    BinaryBlobWriter(Path path, boolean gzip, OutputStream out) throws IOException {
      this.path = path;
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
      byte[] header =
          Serialization.Header.newBuilder()
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
          .setValue(
              data.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(data.getValue()))
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

    @Override
    public Path getPath() {
      return path;
    }
  }

  public static class BinaryBlobReader implements Reader {

    private final Path path;
    private final Parser<Serialization.Element> parser = Serialization.Element.parser();
    private final EntityDescriptor entity;
    private final Serialization.Header header;
    private final String blobName;
    private DataInputStream blobStream = null;

    private BinaryBlobReader(Path path, EntityDescriptor entity, InputStream in)
        throws IOException {
      this.path = path;
      this.entity = entity;
      this.blobName = path.toFile().getAbsolutePath();
      header = readHeader(blobName, in);
      blobStream = toInputStream(in);
    }

    @Override
    public Iterator<StreamElement> iterator() {
      return new AbstractIterator<StreamElement>() {

        @Override
        protected StreamElement computeNext() {
          StreamElement next;
          try {
            next = BinaryBlobReader.this.next();
          } catch (EOFException eof) {
            log.debug("EOF while reading {}. Terminating iteration.", blobName, eof);
            // terminate
            next = null;
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
          if (next != null) {
            return next;
          }
          endOfData();
          return null;
        }
      };
    }

    private byte[] readBytes(DataInputStream in) throws IOException {
      byte[] buf = new byte[in.readInt()];
      in.readFully(buf);
      return buf;
    }

    private Serialization.Header readHeader(String blobName, InputStream in) throws IOException {

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
        log.warn("EOF while reading input of {}. Probably corrupt input?", blobName, eof);
        return Serialization.Header.getDefaultInstance();
      }
    }

    private StreamElement next() throws IOException {
      try {
        return fromBytes(readBytes(blobStream));
      } catch (EOFException eof) {
        log.trace("EOF while reading next data from blob {}.", blobName, eof);
        return null;
      }
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
          return StreamElement.deleteWildcard(
              entity,
              getAttr(parsed),
              parsed.getUuid(),
              parsed.getKey(),
              parsed.getAttribute(),
              parsed.getStamp());
        }
        return StreamElement.delete(
            entity,
            getAttr(parsed),
            parsed.getUuid(),
            parsed.getKey(),
            parsed.getAttribute(),
            parsed.getStamp());
      }
      return StreamElement.upsert(
          entity,
          getAttr(parsed),
          parsed.getUuid(),
          parsed.getKey(),
          parsed.getAttribute(),
          parsed.getStamp(),
          parsed.getValue().toByteArray());
    }

    private AttributeDescriptor<?> getAttr(Serialization.Element parsed) {
      return entity
          .findAttribute(parsed.getAttribute())
          .orElseThrow(
              () -> new IllegalArgumentException("Unknown attribute " + parsed.getAttribute()));
    }

    @Override
    public void close() {
      if (blobStream != null) {
        ExceptionUtils.unchecked(blobStream::close);
        blobStream = null;
      }
    }

    @Override
    public Path getPath() {
      return path;
    }
  }

  /**
   * Create writer from given {@link OutputStream}.
   *
   * @param path path on target FileSystem
   * @param gzip {@code true} if the output be gzipped
   * @param out the {@link OutputStream}
   * @return writer
   * @throws IOException on IO errors
   */
  public static BinaryBlobWriter writer(Path path, boolean gzip, OutputStream out)
      throws IOException {

    return new BinaryBlobWriter(path, gzip, out);
  }

  public BinaryBlobWriter writer(boolean gzip) throws IOException {
    return new BinaryBlobWriter(path, gzip, new FileOutputStream(path.toFile()));
  }

  /**
   * Create reader from given entity and {@link InputStream}.
   *
   * @param path Path on target FileSystem
   * @param entity the entity to read attributes for
   * @param in the {@link InputStream}
   * @return reader
   * @throws IOException on IO errors
   */
  public static BinaryBlobReader reader(Path path, EntityDescriptor entity, InputStream in)
      throws IOException {

    return new BinaryBlobReader(path, entity, in);
  }

  public BinaryBlobReader reader(EntityDescriptor entity) throws IOException {
    return new BinaryBlobReader(path, entity, new FileInputStream(path.toFile()));
  }

  @Getter private final Path path;

  public BinaryBlob(Path path) {
    this.path = path;
  }

  /** Tool for dumping binary blobs read from stdin to stdout. */
  public static class DumpTool {

    private static void usage() {
      System.err.println("Usage: DumpTool <entity name>");
      System.err.println("Reads binary blob from stdin and dumps to stdout");
      System.exit(1);
    }

    public static void main(String[] args) throws IOException {
      if (args.length != 1) {
        usage();
      }
      Repository repo = ConfigRepository.of(ConfigFactory.load().resolve());
      EntityDescriptor entity =
          repo.findEntity(args[0])
              .orElseThrow(() -> new IllegalArgumentException("Cannot find entity " + args[0]));
      Path stdin = Path.stdin();
      try (Reader reader = stdin.openReader(entity)) {
        reader.forEach(e -> System.out.println(e.dump()));
      }
    }
  }
}
