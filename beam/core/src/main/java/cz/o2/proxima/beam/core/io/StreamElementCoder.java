/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core.io;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

public class StreamElementCoder extends CustomCoder<StreamElement> {

  private static final long serialVersionUID = 1L;

  private enum Type {
    UPDATE,
    DELETE,
    DELETE_WILDCARD
  }

  /**
   * Create coder for StreamElements originating in given {@link Repository}.
   *
   * @param factory the repository factory to create coder for
   * @return the coder
   */
  public static StreamElementCoder of(RepositoryFactory factory) {
    return new StreamElementCoder(factory);
  }

  /**
   * Create coder for StreamElements originating in given {@link Repository}.
   *
   * @param repository the repository to create coder for
   * @return the coder
   */
  public static StreamElementCoder of(Repository repository) {
    return new StreamElementCoder(repository.asFactory());
  }

  private final RepositoryFactory repository;

  private StreamElementCoder(RepositoryFactory repository) {
    this.repository = repository;
  }

  @Override
  public void encode(StreamElement value, OutputStream outStream) throws IOException {

    final DataOutput output = new DataOutputStream(outStream);
    output.writeUTF(value.getEntityDescriptor().getName());
    if (value.hasSequentialId()) {
      output.writeUTF("");
      output.writeLong(value.getSequentialId());
    } else {
      output.writeUTF(value.getUuid());
    }
    output.writeUTF(value.getKey());
    final Type type;
    if (value.isDelete()) {
      type = value.isDeleteWildcard() ? Type.DELETE_WILDCARD : Type.DELETE;
    } else {
      type = Type.UPDATE;
    }
    output.writeInt(type.ordinal());
    String attribute = value.getAttribute();
    output.writeUTF(attribute == null ? value.getAttributeDescriptor().getName() : attribute);
    output.writeLong(value.getStamp());
    writeBytes(value.getValue(), output);
  }

  @Override
  public StreamElement decode(InputStream inStream) throws IOException {

    final DataInput input = new DataInputStream(inStream);

    final String entityName = input.readUTF();
    final EntityDescriptor entityDescriptor =
        repository
            .apply()
            .findEntity(entityName)
            .orElseThrow(
                () -> new IOException(String.format("Unable to find entity [%s].", entityName)));

    final String uuid = input.readUTF();
    final long sequentialId = uuid.length() == 0 ? input.readLong() : -1L;
    final String key = input.readUTF();
    final int typeOrdinal = input.readInt();
    final Type type = Type.values()[typeOrdinal];
    String attributeName = input.readUTF();
    if (type.equals(Type.DELETE_WILDCARD)) {
      attributeName = attributeName.substring(0, attributeName.length() - 1);
    }
    final String attribute = attributeName;

    AttributeDescriptor<?> attributeDescriptor =
        entityDescriptor
            .findAttribute(attribute, true)
            .orElseThrow(
                () ->
                    new IOException(
                        String.format(
                            "Unable to find attribute [%s] of entity [%s].",
                            attribute, entityName)));
    final long stamp = input.readLong();

    byte[] value = readBytes(input);
    if (sequentialId == -1L) {
      switch (type) {
        case DELETE_WILDCARD:
          return StreamElement.deleteWildcard(
              entityDescriptor, attributeDescriptor, uuid, key, stamp);
        case DELETE:
          return StreamElement.delete(
              entityDescriptor, attributeDescriptor, uuid, key, attribute, stamp);
        case UPDATE:
          return StreamElement.upsert(
              entityDescriptor, attributeDescriptor, uuid, key, attribute, stamp, value);
        default:
          throw new IllegalStateException("Unknown type " + type);
      }
    } else {
      switch (type) {
        case DELETE_WILDCARD:
          return StreamElement.deleteWildcard(
              entityDescriptor, attributeDescriptor, sequentialId, key, stamp);
        case DELETE:
          return StreamElement.delete(
              entityDescriptor, attributeDescriptor, sequentialId, key, attribute, stamp);
        case UPDATE:
          return StreamElement.upsert(
              entityDescriptor, attributeDescriptor, sequentialId, key, attribute, stamp, value);
        default:
          throw new IllegalStateException("Unknown type " + type);
      }
    }
  }

  @Override
  public TypeDescriptor<StreamElement> getEncodedTypeDescriptor() {
    return TypeDescriptor.of(StreamElement.class);
  }

  private static void writeBytes(@Nullable byte[] value, DataOutput output) throws IOException {

    if (value == null) {
      output.writeInt(-1);
    } else {
      output.writeInt(value.length);
      output.write(value);
    }
  }

  private static @Nullable byte[] readBytes(DataInput input) throws IOException {
    int length = input.readInt();
    if (length >= 0) {
      byte[] ret = new byte[length];
      input.readFully(ret);
      return ret;
    }
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof StreamElementCoder;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public void verifyDeterministic() {}
}
