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

import cz.o2.proxima.storage.Partition;
import java.io.Serializable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A consumer group with name.
 * The consumer group balances assignment of partitions for consumers.
 */
@Slf4j
public class ConsumerGroup implements Serializable {

  /** Assignment of partitions of single consumer. */
  class Assignment implements Serializable {

    @Getter
    final int id;

    @Getter
    final ConsumerRebalanceListener listener;

    @Getter
    final Collection<Partition> partitions = new ArrayList<>();

    Assignment(
        int id,
        ConsumerRebalanceListener listener) {

      this.id = id;
      this.listener = listener != null ? listener : new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> clctn) {
          // nop
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> clctn) {
          // nop
        }
      };
    }

    /** Drop all partitions. */
    void drop() {
      listener.onPartitionsRevoked(partitions.stream()
          .map(p -> new TopicPartition(topic, p.getId()))
          .collect(Collectors.toList()));
      partitions.clear();
    }

    /**
     * Assign given partitions.
     */
    void assign(Collection<Partition> assign) {
      // for sure
      partitions.clear();
      partitions.addAll(assign);
      listener.onPartitionsAssigned(partitions.stream()
          .map(p -> new TopicPartition(topic, p.getId()))
          .collect(Collectors.toList()));
      if (log.isDebugEnabled()) {
        log.debug("Assigned partitions {} to consumer ID {} of group {}",
            partitions.stream().map(Partition::getId).collect(Collectors.toList()),
            id, name);
      }
    }

  }

  /** Name of the group. */
  @Getter
  final String name;

  /** Name of topic of this group. */
  @Getter
  final String topic;

  /** Total number of partitions. */
  @Getter
  final int numPartitions;

  /**
   * Current assignment of partitions into consumers.
   * Key is ID of the consumer.
   */
  final NavigableMap<Integer, Assignment> assignments = new TreeMap<>();


  ConsumerGroup(String name, String topic, int numPartitions) {
    this.name = name;
    this.topic = topic;
    this.numPartitions = numPartitions;
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Number of partitions must be strictly positive");
    }
  }


  /**
   * Add a new consumer with no listener.
   */
  public int add() {
    return add((ConsumerRebalanceListener) null);
  }

  /**
   * Add new consumer to the group.
   * @returns the ID of the newly created consumer.
   */
  public synchronized int add(@Nullable ConsumerRebalanceListener listener) {

    int id = assignments.isEmpty() ? 0 : assignments.lastKey() + 1;
    Assignment assignment = new Assignment(id, listener);
    assignments.put(id, assignment);
    assign(assignments);
    if (log.isDebugEnabled()) {
      log.debug("Added new consumer to group {} with assignments {}",
          name, assignment.getPartitions().stream()
              .map(Partition::getId).collect(Collectors.toList()));
    }
    return id;
  }

  /**
   * Add new consumer to the group with unreassignable partitions.
   */
  public synchronized int add(Collection<Partition> partitions) {

    int id = assignments.isEmpty() ? 0 : assignments.lastKey() + 1;
    Assignment assignment = new Assignment(id, null);
    assignments.put(id, assignment);
    assignment.assign(partitions);
    return id;
  }

  /**
   * Remove consumer from group by id.
   */
  public synchronized void remove(int id) {
    Assignment removed = assignments.remove(id);
    if (removed != null) {
      removed.drop();
      assign(assignments);
    }
  }

  /** Perform a fresh assignment to all the consumers. */
  private void assign(NavigableMap<Integer, Assignment> assignments) {
    if (!assignments.isEmpty()) {
      // drop all assignments
      assignments.values().forEach(Assignment::drop);
      double equalShare = numPartitions / (double) assignments.size();
      double shared = 0.0;
      int partition = 0;
      Iterator<Map.Entry<Integer, Assignment>> iter = assignments.entrySet().iterator();
      while (partition < numPartitions) {
        Map.Entry<Integer, Assignment> next = iter.next();
        List<Partition> partitions = new ArrayList<>();
        shared += equalShare;
        int last = (int) shared;
        while (partition < last) {
          int partitionId = (int) partition++;
          partitions.add(() -> partitionId);
        }
        next.getValue().assign(partitions);
      }
    }
  }


  /**
   * Retrieve partitions for given consumer ID.
   */
  synchronized Collection<Partition> getAssignment(int id) {
    Assignment assignment = assignments.get(id);
    if (assignment != null) {
      return assignment.getPartitions();
    }
    return Collections.emptyList();
  }

  /**
   * Manually assign given partitions to given consumer ID.
   */
  synchronized void assign(int id, List<Partition> assignment) {
    Assignment current = this.assignments.get(id);
    current.drop();
    current.assign(assignment);
  }


}
