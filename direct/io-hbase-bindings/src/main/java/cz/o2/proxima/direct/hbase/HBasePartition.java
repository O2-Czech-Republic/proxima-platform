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
package cz.o2.proxima.direct.hbase;

import cz.o2.proxima.storage.Partition;
import lombok.Getter;

/** A {@code Partition} of data in HBase. */
class HBasePartition implements Partition {

  private static final long serialVersionUID = 1L;

  final int id;
  @Getter final byte[] startKey;
  @Getter final byte[] endKey;
  @Getter final long startStamp;
  @Getter final long endStamp;

  HBasePartition(int id, byte[] startKey, byte[] endKey, long startStamp, long endStamp) {

    this.id = id;
    this.startKey = startKey;
    this.endKey = endKey;
    this.startStamp = startStamp;
    this.endStamp = endStamp;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return "HBasePartition("
        + "id: "
        + id
        + ", startKey: "
        + new String(startKey)
        + ", endKey: "
        + new String(endKey)
        + ", startStamp: "
        + startStamp
        + ", endStamp: "
        + endStamp
        + ")";
  }
}
