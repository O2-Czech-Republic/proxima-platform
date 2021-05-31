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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import cz.o2.proxima.annotations.Internal;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Internal
@ToString
@EqualsAndHashCode
public class State implements Serializable {

  public static State open(long sequentialId, long stamp, Collection<KeyAttribute> openAttributes) {
    return new State(
        sequentialId, stamp, Flags.OPEN, Sets.newHashSet(openAttributes), Collections.emptyList());
  }

  public static State empty() {
    return new State(
        -1L, Long.MIN_VALUE, Flags.UNKNOWN, Collections.emptyList(), Collections.emptyList());
  }

  public enum Flags {
    UNKNOWN,
    OPEN,
    COMMITTED,
    ABORTED
  }

  @Getter private final long sequentialId;
  @Getter private final long stamp;
  @Getter private final Flags flags;
  @Getter private final Collection<KeyAttribute> inputAttributes;
  @Getter private final Collection<KeyAttribute> committedAttributes;

  public State() {
    this(-1L, Long.MIN_VALUE, Flags.UNKNOWN, Collections.emptySet(), Collections.emptySet());
  }

  private State(
      long sequentialId,
      long stamp,
      Flags flags,
      Collection<KeyAttribute> inputAttributes,
      Collection<KeyAttribute> committedAttributes) {

    this.sequentialId = sequentialId;
    this.stamp = stamp;
    this.flags = flags;
    this.inputAttributes = Lists.newArrayList(inputAttributes);
    this.committedAttributes = Lists.newArrayList(committedAttributes);
  }

  /**
   * Create new {@link State} that is marked as {@link Flags#COMMITTED}.
   *
   * @param outputAttributes the attributes that are to be committed
   * @return new {@link State} marked as {@link Flags#COMMITTED}.
   */
  public State committed(Collection<KeyAttribute> outputAttributes) {
    return new State(
        sequentialId,
        stamp,
        Flags.COMMITTED,
        getInputAttributes(),
        Sets.newHashSet(outputAttributes));
  }

  public State update(Collection<KeyAttribute> additionalAttributes) {
    Preconditions.checkState(
        flags == Flags.OPEN, "Cannot update transaction in state %s, expected OPEN", flags);
    Set<KeyAttribute> inputs = Sets.newHashSet(getInputAttributes());
    inputs.addAll(additionalAttributes);
    return new State(sequentialId, stamp, flags, inputs, Collections.emptyList());
  }

  /**
   * Move this state to {@link Flags#ABORTED}.
   *
   * @return new {@link State} marked as {@link Flags#ABORTED}.
   */
  public State aborted() {
    return new State(
        sequentialId, stamp, Flags.ABORTED, getInputAttributes(), Collections.emptySet());
  }
}
