/*
 * Copyright 2017-2026 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.tools.groovy;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.RepositoryFactory;
import cz.o2.proxima.core.storage.StreamElement;
import java.io.Serializable;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Kryo;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Serializer;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Input;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Output;

public class StreamElementSerializer extends Serializer<StreamElement> implements Serializable {

  private final RepositoryFactory factory;
  private transient Repository repoInstance = null;

  public StreamElementSerializer(RepositoryFactory factory) {
    this.factory = factory;
  }

  @Override
  public void write(Kryo kryo, Output output, StreamElement element) {
    if (element.hasSequentialId()) {
      output.writeBoolean(true);
      output.writeVarLong(element.getSequentialId(), true);
    } else {
      output.writeBoolean(false);
      output.writeString(element.getUuid());
    }
    output.writeString(element.getEntityDescriptor().getName());
    output.writeString(element.getKey());
    output.writeString(element.getAttribute());
    output.writeVarLong(element.getStamp(), true);
    output.writeVarInt(element.isDelete() ? -1 : element.getValue().length, false);
    if (!element.isDelete()) {
      output.writeBytes(element.getValue());
    }
  }

  @Override
  public StreamElement read(Kryo kryo, Input input, Class<? extends StreamElement> aClass) {
    final Repository repo = repo();
    final long seqId;
    final String uuid;
    if (input.readBoolean()) {
      seqId = input.readVarLong(true);
      uuid = null;
    } else {
      seqId = -1;
      uuid = input.readString();
    }
    final String entity = input.readString();
    final String key = input.readString();
    final String attribute = input.readString();
    final long stamp = input.readVarLong(true);
    final EntityDescriptor entityDesc = repo.getEntity(entity);
    final AttributeDescriptor<?> attrDesc = entityDesc.getAttribute(attribute);
    int len = input.readVarInt(false);
    if (len < 0) {
      if (seqId >= 0) {
        return StreamElement.delete(entityDesc, attrDesc, seqId, key, attribute, stamp);
      }
      return StreamElement.delete(entityDesc, attrDesc, uuid, key, attribute, stamp);
    }
    if (seqId >= 0) {
      return StreamElement.upsert(
          entityDesc, attrDesc, seqId, key, attribute, stamp, input.readBytes(len));
    }
    return StreamElement.upsert(
        entityDesc, attrDesc, uuid, key, attribute, stamp, input.readBytes(len));
  }

  private Repository repo() {
    if (repoInstance == null) {
      repoInstance = factory.apply();
    }
    return repoInstance;
  }
}
