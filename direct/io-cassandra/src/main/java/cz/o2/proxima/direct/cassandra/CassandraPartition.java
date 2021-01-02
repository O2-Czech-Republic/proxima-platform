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
package cz.o2.proxima.direct.cassandra;

import cz.o2.proxima.direct.batch.BoundedPartition;
import lombok.Getter;

/** A {@code Partition} in Cassandra. */
public class CassandraPartition extends BoundedPartition {

  private static final long serialVersionUID = 1L;

  @Getter final long minStamp;

  @Getter final long maxStamp;

  @Getter final long tokenStart;

  @Getter final long tokenEnd;

  @Getter final boolean endInclusive;

  public CassandraPartition(
      int id, long minStamp, long maxStamp, long tokenStart, long tokenEnd, boolean endInclusive) {

    super(id);
    this.minStamp = minStamp;
    this.maxStamp = maxStamp;
    this.tokenStart = tokenStart;
    this.tokenEnd = tokenEnd;
    this.endInclusive = endInclusive;
  }
}
