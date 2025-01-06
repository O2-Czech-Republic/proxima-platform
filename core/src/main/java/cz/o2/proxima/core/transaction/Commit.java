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

import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;

/**
 * A commit request sent in case there are multiple output attributes written in a transaction. When
 * there is only single attribute, the output is written directly to target commit-log.
 */
@ToString
@Getter
@EqualsAndHashCode
public class Commit implements Serializable {

  /** An update to transactional attribute of a transaction. */
  @Value
  public static class TransactionUpdate implements Serializable {
    /** Name of target attribute family. */
    String targetFamily;

    /** The update itself. */
    StreamElement update;
  }

  /**
   * Create new {@link Commit} message to be appended to {@code _transcation.commit}
   *
   * @param outputs updates to write
   * @return the commit
   */
  public static Commit outputs(
      Collection<TransactionUpdate> transactionUpdates, Collection<StreamElement> outputs) {
    Preconditions.checkArgument(
        outputs.stream().noneMatch(StreamElement::isDeleteWildcard),
        "Wildcard deletes not currently supported.");
    Preconditions.checkArgument(
        outputs.stream().allMatch(el -> el.getStamp() > 0 && el.getSequentialId() > 0),
        "Invalid updates in %s",
        outputs);
    return new Commit(outputs, transactionUpdates);
  }

  /**
   * Create new {@link Commit} message to be appended to {@code _transcation.commit}
   *
   * @param transactionUpdates Updates to transactional attributes.
   */
  public static Commit updates(Collection<TransactionUpdate> transactionUpdates) {
    Preconditions.checkArgument(
        transactionUpdates.stream().noneMatch(u -> u.getUpdate().isDelete()),
        "Deletes on transactional attributes not supported.");
    return new Commit(Collections.emptyList(), transactionUpdates);
  }

  /** List of {@link cz.o2.proxima.core.storage.StreamElement StreamElements} to be replicated. */
  private final Collection<StreamElement> outputs;

  /** List of possible state updates and/or responses to return to client. */
  private final Collection<TransactionUpdate> transactionUpdates;

  public Commit() {
    this.outputs = Collections.emptyList();
    this.transactionUpdates = Collections.emptyList();
  }

  private Commit(
      Collection<StreamElement> outputs, Collection<TransactionUpdate> transactionUpdates) {

    this.outputs = outputs;
    this.transactionUpdates = new ArrayList<>(transactionUpdates);
  }
}
