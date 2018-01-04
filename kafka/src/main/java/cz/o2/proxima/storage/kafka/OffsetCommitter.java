/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage.kafka;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A committer of kafka offsets. This committer is used for asynchronous
 * operations to enforce that the offset is not committed earlier then all
 * associated actions are performed.
 */
@Slf4j
public class OffsetCommitter<ID> {

  /**
   * Callback to be called for performing the commit.
   */
  @FunctionalInterface
  public static interface  Callback {
    void apply();
  }

  /**
   * A single offset with metadata.
   */
  static class OffsetMeta {

    AtomicInteger actions;   // counter for number of actions

    @Getter
    Callback commit;        // the associated commit callback

    OffsetMeta(int actions, Callback commit) {
      this.actions = new AtomicInteger(actions);
      this.commit = commit;
    }

    int getActions() {
      return actions.get();
    }

    synchronized void decrement() {
      if (actions.decrementAndGet() < 0) {
        log.error("Decremented too many, actions now {}", actions);
      }
    }

    @Override
    public String toString() {
      return "OffsetMeta(actions=" + actions + ")";
    }


  }

  final Map<ID, NavigableMap<Long, OffsetMeta>> waitingOffsets;

  public OffsetCommitter() {
    waitingOffsets = Collections.synchronizedMap(new HashMap<>());
  }

  /**
   * Register number of actions to be performed before offset can be committed.
   */
  public void register(ID id, long offset, int numActions, Callback commit) {
    NavigableMap<Long, OffsetMeta> current = waitingOffsets.get(id);
    log.debug(
        "Registering offset {} for ID {} with {} actions",
        offset, id, numActions);
    if (current == null) {
      current = Collections.synchronizedNavigableMap(new TreeMap<>());
      waitingOffsets.put(id, current);
    }
    current.put(offset, new OffsetMeta(numActions, commit));
    if (numActions == 0) {
      checkCommitable(id, current);
    }
  }

  /**
   * Confirm that action associated with given offset has been performed.
   */
  public void confirm(ID id, long offset) {
    NavigableMap<Long, OffsetMeta> current = waitingOffsets.get(id);
    log.debug("Confirming processing of offset {} with ID {}", offset, id);
    if (current != null) {
      OffsetMeta meta = current.get(offset);
      if (meta != null) {
        meta.decrement();
      }
      checkCommitable(id, current);
    }
  }

  private void checkCommitable(ID id, NavigableMap<Long, OffsetMeta> current) {
    synchronized (current) {
      List<Map.Entry<Long, OffsetMeta>> commitable = new ArrayList<>();
      for (Map.Entry<Long, OffsetMeta> e : current.entrySet()) {
        if (e.getValue().getActions() <= 0) {
          log.debug(
              "Adding offset {} of ID {} to committable map.",
              e.getKey(), id);
          commitable.add(e);
        } else {
          log.debug(
              "Waiting for still non-committed offset {}, {} actions missing",
              e.getKey(), e.getValue().getActions());
          break;
        }
      }
      if (!commitable.isEmpty()) {
        Map.Entry<Long, OffsetMeta> toCommit = commitable.get(commitable.size() - 1);
        toCommit.getValue().getCommit().apply();
        commitable.forEach(e -> current.remove(e.getKey()));
      }
    }
  }

  /**
   * Clear all records for given topic partition and offset.
   */
  public void clear(ID id, long offset) {
    NavigableMap<Long, OffsetMeta> current = waitingOffsets.get(id);
    if (current != null) {
      current.remove(offset);
    }
  }

}
