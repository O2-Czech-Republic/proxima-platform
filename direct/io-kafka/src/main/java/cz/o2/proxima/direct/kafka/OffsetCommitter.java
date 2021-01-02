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
package cz.o2.proxima.direct.kafka;

import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * A committer of kafka offsets. This committer is used for asynchronous operations to enforce that
 * the offset is not committed earlier then all associated actions are performed.
 */
@Slf4j
public class OffsetCommitter<ID> {

  /** Callback to be called for performing the commit. */
  @FunctionalInterface
  public interface Callback {
    void apply();
  }

  /** A single offset with metadata. */
  static class OffsetMeta {

    AtomicInteger actions; // counter for number of actions

    @Getter Callback commit; // the associated commit callback

    @Getter final long createdNanos = System.nanoTime();

    OffsetMeta(int actions, Callback commit) {
      this.actions = new AtomicInteger(actions);
      this.commit = commit;
    }

    int getActions() {
      return actions.get();
    }

    long getNanoAge() {
      return System.nanoTime() - createdNanos;
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

  private final Map<ID, NavigableMap<Long, OffsetMeta>> waitingOffsets;
  private final long stateCommitWarningNanos;
  private final long autoCommitNanos;

  public OffsetCommitter() {
    this(60000000000L, Long.MAX_VALUE);
  }

  public OffsetCommitter(long staleCommitWarningNanos, long autoCommitNanos) {
    this.waitingOffsets = Collections.synchronizedMap(new HashMap<>());
    this.stateCommitWarningNanos = staleCommitWarningNanos;
    this.autoCommitNanos = autoCommitNanos;
  }

  /**
   * Register number of actions to be performed before offset can be committed.
   *
   * @param id id of the consumer
   * @param offset the registered offset
   * @param numActions how many times {@link #confirm(Object, long)} should be called to consider
   *     the action as done
   * @param commit {@link Callback} to call to commit
   */
  public void register(ID id, long offset, int numActions, Callback commit) {
    NavigableMap<Long, OffsetMeta> current =
        waitingOffsets.computeIfAbsent(
            id, tmp -> Collections.synchronizedNavigableMap(new TreeMap<>()));
    log.debug("Registered offset {} for ID {} with {} actions", offset, id, numActions);
    current.put(offset, new OffsetMeta(numActions, commit));
    if (numActions == 0) {
      checkCommittable(id, current);
    }
  }

  /**
   * Confirm that action associated with given offset has been performed.
   *
   * @param id id of the consumer
   * @param offset the offset to confirm
   */
  public void confirm(ID id, long offset) {
    Map<Long, OffsetMeta> current = waitingOffsets.get(id);
    log.debug("Confirming processing of offset {} with ID {}", offset, id);
    if (current != null) {
      OffsetMeta meta = current.get(offset);
      if (meta != null) {
        meta.decrement();
      }
      checkCommittable(id, current);
    }
  }

  private void checkCommittable(ID id, final Map<Long, OffsetMeta> current) {
    synchronized (current) {
      List<Map.Entry<Long, OffsetMeta>> committable = new ArrayList<>();
      for (Map.Entry<Long, OffsetMeta> e : current.entrySet()) {
        long age = e.getValue().getNanoAge();
        if (age > autoCommitNanos) {
          committable.add(e);
          log.warn(
              "Auto adding offset {} of ID {} to comittable map due to age {} ns. "
                  + "The commit might have been lost. Verify your commit logic to remove this warning.",
              e.getKey(),
              id,
              age);
        } else if (e.getValue().getActions() <= 0) {
          committable.add(e);
          log.debug("Added offset {} of ID {} to committable map.", e.getKey(), id);
        } else {
          if (age > stateCommitWarningNanos) {
            log.warn(
                "Offset {} ID {} was not committed in {} ns ({} actions missing). Please verify your commit logic!",
                e.getKey(),
                id,
                age,
                e.getValue().getActions());
          } else {
            log.debug(
                "Waiting for still non-committed offset {} in {}, {} actions missing",
                e.getKey(),
                id,
                e.getValue().getActions());
          }
          break;
        }
      }
      if (!committable.isEmpty()) {
        Map.Entry<Long, OffsetMeta> toCommit = committable.get(committable.size() - 1);
        committable.forEach(e -> current.remove(e.getKey()));
        toCommit.getValue().getCommit().apply();
      }
    }
  }

  /**
   * Clear all records for given topic partition and offset.
   *
   * @param id id of the consumer
   * @param offset offset to clear
   */
  public void clear(ID id, long offset) {
    Map<Long, OffsetMeta> current = waitingOffsets.get(id);
    if (current != null) {
      current.remove(offset);
    }
  }

  /** Clear completely all mappings. */
  public void clear() {
    waitingOffsets.clear();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass()).add("waitingOffsets", waitingOffsets).toString();
  }
}
