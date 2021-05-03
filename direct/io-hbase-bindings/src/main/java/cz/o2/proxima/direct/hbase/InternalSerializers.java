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
package cz.o2.proxima.direct.hbase;

import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RawOffset;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

class InternalSerializers {

  static Put toPut(byte[] family, byte[] keyAsBytes, StreamElement element, byte[] rawValue) {
    String column = element.getAttribute();
    Put put = new Put(keyAsBytes, element.getStamp());
    put.addColumn(family, column.getBytes(StandardCharsets.UTF_8), element.getStamp(), rawValue);
    return put;
  }

  static <V> KeyValue<V> toKeyValue(
      EntityDescriptor entity,
      AttributeDescriptor<V> attrDesc,
      Cell cell,
      long seqId,
      byte[] rawValue) {

    String key = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    String attribute =
        new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

    if (seqId > 0) {
      return KeyValue.of(
          entity,
          attrDesc,
          seqId,
          key,
          attribute,
          new RawOffset(attribute),
          rawValue,
          cell.getTimestamp());
    }
    return KeyValue.of(
        entity, attrDesc, key, attribute, new RawOffset(attribute), rawValue, cell.getTimestamp());
  }
}
