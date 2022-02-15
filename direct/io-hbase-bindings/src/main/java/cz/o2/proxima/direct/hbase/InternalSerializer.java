/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.hbase;

import static cz.o2.proxima.direct.hbase.Util.cloneArray;

import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.io.serialization.shaded.com.google.protobuf.ByteString;
import cz.o2.proxima.io.serialization.shaded.com.google.protobuf.InvalidProtocolBufferException;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

interface InternalSerializer {

  class V1Serializer implements InternalSerializer {

    @Override
    public Put toPut(byte[] family, byte[] keyAsBytes, StreamElement element) {
      return InternalSerializers.toPut(family, keyAsBytes, element, element.getValue());
    }

    @Override
    public <V> KeyValue<V> toKeyValue(
        EntityDescriptor entity, AttributeDescriptor<V> attrDesc, Cell cell) {

      return InternalSerializers.toKeyValue(
          entity,
          attrDesc,
          cell,
          -1L,
          cloneArray(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    }
  }

  class V2Serializer implements InternalSerializer {

    @Override
    public Put toPut(byte[] family, byte[] keyAsBytes, StreamElement element) {
      cz.o2.proxima.storage.proto.Serialization.Cell.Builder builder =
          cz.o2.proxima.storage.proto.Serialization.Cell.newBuilder()
              .setValue(ByteString.copyFrom(element.getValue()));
      if (element.hasSequentialId()) {
        builder.setSeqId(element.getSequentialId());
      }
      return InternalSerializers.toPut(family, keyAsBytes, element, builder.build().toByteArray());
    }

    @Override
    public <V> KeyValue<V> toKeyValue(
        EntityDescriptor entity, AttributeDescriptor<V> attrDesc, Cell cell)
        throws InvalidProtocolBufferException {

      cz.o2.proxima.storage.proto.Serialization.Cell protoCell =
          cz.o2.proxima.storage.proto.Serialization.Cell.parseFrom(
              ByteString.copyFrom(
                  cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
      return InternalSerializers.toKeyValue(
          entity, attrDesc, cell, protoCell.getSeqId(), protoCell.getValue().toByteArray());
    }
  }

  Put toPut(byte[] family, byte[] keyAsBytes, StreamElement element);

  <V> KeyValue<V> toKeyValue(
      EntityDescriptor entityDescriptor, AttributeDescriptor<V> attribute, Cell cell)
      throws IOException;
}
