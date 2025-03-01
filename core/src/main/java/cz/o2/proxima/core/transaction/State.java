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
import cz.o2.proxima.internal.com.google.common.collect.Lists;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
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
  @Getter private final Collection<StreamElement> committedOutputs;

  public State() {
    this(-1L, Long.MIN_VALUE, Flags.UNKNOWN, Collections.emptySet(), Collections.emptySet());
  }

  private State(
      long sequentialId,
      long stamp,
      Flags flags,
      Collection<KeyAttribute> inputAttributes,
      Collection<StreamElement> committedOutputs) {

    this.sequentialId = sequentialId;
    this.stamp = stamp;
    this.flags = flags;
    this.inputAttributes = Lists.newArrayList(inputAttributes);
    this.committedOutputs = Lists.newArrayList(committedOutputs);
  }

  /**
   * Create new {@link State} that is marked as {@link Flags#COMMITTED}.
   *
   * @param outputs the attributes that are to be committed
   * @return new {@link State} marked as {@link Flags#COMMITTED}.
   */
  public State committed(Collection<StreamElement> outputs) {
    return new State(
        sequentialId, stamp, Flags.COMMITTED, getInputAttributes(), Sets.newHashSet(outputs));
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

  public boolean isFinal() {
    return this.flags == Flags.COMMITTED || this.flags == Flags.ABORTED;
  }
}
