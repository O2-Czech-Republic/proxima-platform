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

import com.google.common.base.Preconditions;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * A commit request sent in case there are multiple output attributes written in a transaction. When
 * there is only single attribute, the output is written directly to target commit-log.
 */
@ToString
@EqualsAndHashCode
public class Commit implements Serializable {

  /**
   * Create new {@link Commit} message to be appended to {@code _transcation.commit}
   *
   * @param seqId sequence Id of the transaction
   * @param stamp output timestamp of the transaction
   * @param updates updates to write
   * @return the commit
   */
  public static Commit of(long seqId, long stamp, Collection<StreamElement> updates) {
    Preconditions.checkArgument(
        updates.stream().noneMatch(StreamElement::isDeleteWildcard),
        "Wildcard deletes not currently supported.");
    Preconditions.checkArgument(seqId > 0, "SequenceId must be positive, for %s", seqId);
    return new Commit(seqId, stamp, updates);
  }

  /** Transaction's sequenceId. */
  @Getter private final long seqId;

  /** Transaction's stamp. */
  @Getter private final long stamp;

  /** List of {@link cz.o2.proxima.storage.StreamElement StreamElements} to be replicated. */
  @Getter private final List<StreamElement> updates;

  public Commit() {
    this(-1L, Long.MIN_VALUE, Collections.emptyList());
  }

  private Commit(long seqId, long stamp, Collection<StreamElement> updates) {
    this.seqId = seqId;
    this.stamp = stamp;
    this.updates = fixSeqIdAndStamp(seqId, stamp, updates);
  }

  private static List<StreamElement> fixSeqIdAndStamp(
      long seqId, long stamp, Collection<StreamElement> updates) {

    return updates
        .stream()
        .map(
            s -> {
              Preconditions.checkArgument(
                  !s.isDeleteWildcard(), "Wildcard deletes not yet supported, got %s", s);
              if (s.getSequentialId() != seqId || s.getStamp() != stamp) {
                return StreamElement.upsert(
                    s.getEntityDescriptor(),
                    s.getAttributeDescriptor(),
                    seqId,
                    s.getKey(),
                    s.getAttribute(),
                    stamp,
                    s.getValue());
              }
              return s;
            })
        .collect(Collectors.toList());
  }
}
