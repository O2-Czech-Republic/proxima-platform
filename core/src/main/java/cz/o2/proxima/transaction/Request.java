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
package cz.o2.proxima.transaction;

import cz.o2.proxima.annotations.Internal;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** A transactional request sent to coordinator. */
@Internal
@Builder
@ToString
@EqualsAndHashCode
public class Request implements Serializable {

  public enum Flags {
    NONE,
    OPEN,
    UPDATE,
    ROLLBACK,
    COMMIT;
  }

  @Getter private final List<KeyAttribute> inputAttributes;
  @Getter private final List<KeyAttribute> outputAttributes;
  @Getter private final Flags flags;
  @Getter private final int responsePartitionId;

  public Request() {
    this(null, null, Flags.NONE, -1);
  }

  public Request(
      List<KeyAttribute> inputAttributes,
      List<KeyAttribute> outputAttributes,
      Flags flags,
      int responsePartitionId) {

    this.inputAttributes = inputAttributes == null ? Collections.emptyList() : inputAttributes;
    this.outputAttributes = outputAttributes == null ? Collections.emptyList() : outputAttributes;
    this.flags = flags;
    this.responsePartitionId = responsePartitionId;
  }

  public Request withResponsePartitionId(int responsePartitionId) {
    return new Request(inputAttributes, outputAttributes, flags, responsePartitionId);
  }
}
