/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StorageDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Cancellable;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.util.Pair;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that can be used as {@code KafkaCommitLog} in various test scenarios.
 * The commit log is associated with URIs `kafka-test`.
 */
public class LocalKafkaCommitLogDescriptor extends StorageDescriptor<KafkaCommitLog> {

  private static final Logger LOG = LoggerFactory.getLogger(LocalKafkaCommitLogDescriptor.class);

  public static final String CFG_NUM_PARTITIONS = "local-kafka-num-partitions";

  @Getter
  Map<URI, Accessor> createdWriters = new HashMap<>();

  public class Accessor extends KafkaCommitLog {

    final int numPartitions;

    // list of consumers by name with assigned partitions
    final Map<String, ConsumerGroup> consumerGroups;

    // ingests in different partitions
    final List<List<StreamElement>> written;

    // (consumer name, consumer id) -> list(partition id, offset) (sparse)
    final Map<Pair<String, Integer>, List<Pair<Integer, AtomicInteger>>> consumerOffsets;

    // (consumer name, partition id) -> committed offset
    final Map<Pair<String, Integer>, AtomicInteger> committedOffsets;

    public Accessor(EntityDescriptor entity, URI uri, Map<String, Object> cfg) {
      super(
          entity,
          uri,
          cfg);

      this.consumerOffsets = Collections.synchronizedMap(new HashMap<>());
      this.written = Collections.synchronizedList(new ArrayList<>());
      this.consumerGroups = Collections.synchronizedMap(new HashMap<>());
      this.committedOffsets = Collections.synchronizedMap(new HashMap<>());

      numPartitions = Optional.ofNullable(cfg.get(CFG_NUM_PARTITIONS))
          .filter(o -> o != null)
          .map(o -> Integer.valueOf(o.toString()))
          .orElse(1);

      for (int i = 0; i < numPartitions; i++) {
        written.add(Collections.synchronizedList(new ArrayList<>()));
      }

      LOG.info(
          "Created LocalKafkaCommitLog with URI {}, partitioner {} and {} partitions",
          uri, partitioner.getClass().getName(), numPartitions);
    }

    public LocalKafkaCommitLogDescriptor getDescriptor() {
      return LocalKafkaCommitLogDescriptor.this;
    }

    @Override
    public KafkaConsumerFactory createConsumerFactory() {

      return new KafkaConsumerFactory(getURI(), new Properties()) {

        @Override
        public KafkaConsumer<String, byte[]> create() {
          return create(allPartitions());
        }

        @Override
        public KafkaConsumer<String, byte[]> create(Collection<Partition> partitions) {
          String name = "unnamed-consumer-" + Math.round(Math.random() * Integer.MAX_VALUE);
          ConsumerGroup group = new ConsumerGroup(name, getTopic(), numPartitions);
          int id = group.add(partitions);
          return mockKafkaConsumer(
              name,
              Pair.of(name, id),
              group);
        }

        @Override
        public KafkaConsumer<String, byte[]> create(String name) {
          return create(name, null);
        }

        @Override
        public KafkaConsumer<String, byte[]> create(
            String name,
            @Nullable ConsumerRebalanceListener listener) {

          synchronized (LocalKafkaCommitLogDescriptor.this) {
            ConsumerGroup group = consumerGroups.get(name);
            if (group == null) {
              group = new ConsumerGroup(name, getTopic(), numPartitions);
              consumerGroups.put(name, group);
            }

            int id = group.add(listener);
            return mockKafkaConsumer(name, Pair.of(name, id), group);
          }
        }

        private List<Partition> allPartitions() {
          List<Partition> ret = new ArrayList<>();
          for (int i = 0; i < numPartitions; i++) {
            int id = i;
            ret.add(() -> id);
          }
          return ret;
        }

      };
    }


    /**
     * Mock kafka consumer consuming from specified partitions.
     * @param id ID of the consumer in the associated consumer group
     */
    @SuppressWarnings("unchecked")
    protected KafkaConsumer<String, byte[]> mockKafkaConsumer(
        String name,
        Pair<String, Integer> consumerId,
        ConsumerGroup group) {

      for (int p  = 0; p < numPartitions; p++) {
        committedOffsets.putIfAbsent(
            Pair.of(name, p),
            new AtomicInteger(written.get(p).size() - 1));
      }

      LOG.info(
          "Creating mock kafka consumer name {}, with committed offsets {}",
          name,
          committedOffsets);

      KafkaConsumer<String, byte[]> mock = mock(KafkaConsumer.class);
      consumerOffsets.put(
          consumerId,
          group.getAssignment(consumerId.getSecond()).stream()
              .map(p -> Pair.of(p.getId(), new AtomicInteger(
                  committedOffsets.get(Pair.of(name, p.getId())).get())))
              .collect(Collectors.toList()));

      doAnswer(invocation -> {
        long sleep = (long) invocation.getArguments()[0];
        return pollConsumer(group, sleep, consumerId);
      }).when(mock).poll(anyLong());

      doAnswer(invocation -> {
        Collection<TopicPartition> tp;
        tp = (Collection<TopicPartition>) invocation.getArguments()[0];
        return getEndOffsets(name, tp);
      }).when(mock).endOffsets(any());

      doAnswer(invocation -> {
        Collection<TopicPartition> c;
        c = (Collection<TopicPartition>) invocation.getArguments()[0];
        Map<TopicPartition, Long> starts = c.stream()
            .collect(Collectors.toMap(i -> i, i -> 0L));
        LOG.debug("Consumer {} beginningOffsets {}: {}", name, c, starts);
        return starts;
      }).when(mock).beginningOffsets(any());

      when(mock.assignment()).thenReturn(
          Collections.singleton(new TopicPartition(getTopic(), 0)));

      doAnswer(invocation -> {
        Collection<TopicPartition> parts;
        parts = (Collection<TopicPartition>) invocation.getArguments()[0];
        seekConsumerToBeginning(consumerId, parts);
        return null;
      }).when(mock).seekToBeginning(any());

      doAnswer(invocation -> {
        Map<TopicPartition, OffsetAndMetadata> commitMap;
        commitMap = (Map<TopicPartition, OffsetAndMetadata>) invocation.getArguments()[0];
        commitConsumer(name, commitMap);
        return null;
      }).when(mock).commitSync(any());

      doAnswer(invocation -> {
        Collection<Partition> partitions = group.getAssignment(consumerId.getSecond());
        return partitions.stream()
            .map(p -> new TopicPartition(getTopic(), p.getId()))
            .collect(Collectors.toSet());
      }).when(mock).assignment();

      return mock;
    }

    private void commitConsumer(
        String name,
        Map<TopicPartition, OffsetAndMetadata> commitMap) {

      commitMap.entrySet().forEach(entry -> {
        int partition = entry.getKey().partition();
        long offset = entry.getValue().offset();
        committedOffsets.get(Pair.of(name, partition)).set((int) offset);
      });
      LOG.debug("Consumer {} committed offsets {}", name, commitMap);
    }

    private void seekConsumerToBeginning(
        Pair<String, Integer> consumerId,
        Collection<TopicPartition> parts) {

      List<Pair<Integer, AtomicInteger>> partOffsets;
      partOffsets = consumerOffsets.get(consumerId);
      for (TopicPartition tp : parts) {
        for (Pair<Integer, AtomicInteger> p : partOffsets) {
          if (p.getFirst() == tp.partition()) {
            p.getSecond().set(0);
          }
        }
      }
      LOG.debug(
          "Consumer {} seeked to beginning of {}",
          consumerId.getFirst(),
          parts);
    }

    private Map<TopicPartition, Long> getEndOffsets(
        String name,
        Collection<TopicPartition> tp) {
      Map<TopicPartition, Long> ends = new HashMap<>();
      for (TopicPartition p : tp) {
        ends.put(
            new TopicPartition(getTopic(), p.partition()),
            (long) written.get(p.partition()).size());
      }
      LOG.debug("Consumer {} endOffsets {}: {}", name, tp, ends);
      return ends;
    }

    private ConsumerRecords<String, byte[]> pollConsumer(
        ConsumerGroup group,
        long period, Pair<String, Integer> consumerId)
        throws InterruptedException {

      String name = consumerId.getFirst();
      // need to sleep in order not to pollute the heap with
      // unnecessary objects
      // this is because mockito somehow creates lots of objects
      // when it is invoked too often
      Thread.sleep(period);

      Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> map;
      map = new HashMap<>();
      Collection<Partition> assignment = group.getAssignment(consumerId.getSecond());
      List<Pair<Integer, AtomicInteger>> offsets = consumerOffsets.get(consumerId);

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Polling consumerId {} with assignment {}",
            consumerId,
            assignment.stream().map(Partition::getId).collect(Collectors.toList()));
      }
      for (Partition part : assignment) {
        int partition = part.getId();
        List<StreamElement> partitionData = written.get(partition);
        int last = partitionData.size();
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        int off = offsets.stream()
            .filter(p -> p.getFirst() == partition)
            .map(p -> p.getSecond().get())
            .findAny()
            .orElse(committedOffsets.get(Pair.of(name, partition)).get());
        LOG.trace(
            "Partition {} has last {}, reading from {}",
            partition, last, off);

        while (off < last) {
          if (off >= 0) {
            records.add(toConsumerRecord(partitionData.get(off), part.getId(), off));
          }
          off++;
        }

        // advance the offset
        boolean found = false;
        for (Pair<Integer, AtomicInteger> p : offsets) {
          if (p.getFirst() == partition) {
            p.getSecond().set(off);
            LOG.trace(
                "Advanced offset of consumer ID {} on partition {} to {}",
                consumerId, partition, off);
            found = true;
            break;
          }
        }
        if (!found) {
          offsets.add(Pair.of(partition, new AtomicInteger(off)));
        }
        if (!records.isEmpty()) {
          map.put(new TopicPartition(getTopic(), partition), records);
        }
      }
      LOG.debug(
          "Consumer {} id {} polled records {}",
          name,
          consumerId,
          map);

      return new ConsumerRecords<>(map);
    }


    @Override
    public Cancellable observePartitions(
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      Cancellable ret = super.observePartitions(
          null, partitions, position, stopAtCurrent, observer, null);
      LOG.debug(
          "Started to observe partitions {} of LocalKafkaCommitLog URI {}",
          partitions, getURI());
      return ret;
    }

    @Override
    public Cancellable observe(String name, Position position, LogObserver observer) {
      Cancellable ret = super.observe(name, position, observer);
      LOG.debug(
          "Started to observe LocalKafkaCommitLog with URI {} by consumer {}",
          getURI(), name);
      return ret;
    }


    @Override
    public void write(StreamElement data, CommitCallback callback) {
      int partitionId = partitioner.getPartitionId(
          data.getKey(), data.getAttribute(), data.getValue());
      int partition = (partitionId & Integer.MAX_VALUE) % numPartitions;
      LOG.debug(
          "Written data {} to LocalKafkaCommitLog URI {}, partition {}",
          data, getURI(), partition);
      written.get(partition).add(data);
      callback.commit(true, null);
    }

    private ConsumerRecord<String, byte[]> toConsumerRecord(
        StreamElement ingest, int partitionId, int offset) {
      return new ConsumerRecord<>(
          getTopic(),
          partitionId,
          offset,
          System.currentTimeMillis(),
          TimestampType.LOG_APPEND_TIME,
          0L,
          -1,
          -1,
          ingest.getKey() + "#" + ingest.getAttribute(),
          ingest.getValue());
    }

    public boolean allConsumed() {
      return allConsumed(written.stream()
          .map(List::size)
          .collect(Collectors.toList()));
    }

    public boolean allConsumed(List<Integer> untilOffsets) {
      return consumerGroups.keySet().stream().map(name -> {
        int partition = 0;
        for (int written : untilOffsets) {
          int current = partition;
          if (consumerOffsets.get(Pair.of(name, current))
              .stream()
              .filter(off -> off.getFirst() == current)
              .filter(off -> off.getSecond().get() < written)
              .findAny()
              .isPresent())
            return false;
          partition++;
        }
        return true;
      }).reduce(true, (a, b) -> a && b);
    }

  }

  public LocalKafkaCommitLogDescriptor() {
    super(Arrays.asList("kafka-test"));
  }

  @Override
  public LocalKafkaCommitLogDescriptor.Accessor getAccessor(
      EntityDescriptor entityDesc,
      URI uri,
      Map<String, Object> cfg) {

    Accessor writer = createdWriters.get(uri);
    if (writer == null) {
      createdWriters.put(uri, writer = new Accessor(entityDesc, uri, cfg));
    }
    return writer;
  }

}
