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
package cz.o2.proxima.direct.io.cassandra;

import cz.o2.proxima.direct.core.batch.BoundedPartition;
import lombok.EqualsAndHashCode;
import lombok.Value;

/** A {@code Partition} in Cassandra. */
@EqualsAndHashCode(callSuper = true)
@Value
public class CassandraPartition extends BoundedPartition {

  private static final long serialVersionUID = 1L;

  long minStamp;

  long maxStamp;

  long tokenStart;

  long tokenEnd;

  boolean endInclusive;

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
