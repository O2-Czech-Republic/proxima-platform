/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.transaction;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** A transactional request sent to coordinator. */
@Getter
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
    COMMIT
  }

  private final List<KeyAttribute> inputAttributes;
  private final Collection<StreamElement> outputs;
  private final Flags flags;
  private final int responsePartitionId;

  public Request() {
    this(null, null, Flags.NONE, -1);
  }

  public Request(
      List<KeyAttribute> inputAttributes,
      Collection<StreamElement> outputs,
      Flags flags,
      int responsePartitionId) {

    this.inputAttributes = inputAttributes == null ? Collections.emptyList() : inputAttributes;
    this.outputs = outputs == null ? Collections.emptyList() : outputs;
    this.flags = flags;
    this.responsePartitionId = responsePartitionId;

    Preconditions.checkArgument(
        (flags != Flags.UPDATE && flags != Flags.OPEN) || !this.inputAttributes.isEmpty(),
        "Input attributes have to be non-empty wth flags %s.",
        flags);

    Preconditions.checkArgument(
        this.outputs.stream().allMatch(el -> el.getSequentialId() > 0),
        "Outputs of transaction must have associated sequential Id, got %s",
        outputs);
  }

  public Request withResponsePartitionId(int responsePartitionId) {
    return new Request(inputAttributes, outputs, flags, responsePartitionId);
  }
}
