/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import static org.mockito.Mockito.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

/**
 * A class that can be used as {@code KafkaCommitLog} in various test scenarios. The commit log is
 * associated with URIs `kafka-test`.
 */
@Slf4j
public class LocalKafkaCommitLogDescriptor implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  public static final String CFG_NUM_PARTITIONS = "local-kafka-num-partitions";

  // we need this to be able to survive serialization
  private static final Map<String, Map<URI, Accessor>> ACCESSORS = new ConcurrentHashMap<>();

  // identifier of consumer with group name and consumer Id
  private static class ConsumerId {
    static ConsumerId of(String name, int id) {
      return new ConsumerId(name, id);
    }

    @Getter final String name;
    @Getter final int id;
    @Getter @Setter boolean assigned = false;

    private ConsumerId(String name, int id) {
      this.name = name;
      this.id = id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, id);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ConsumerId)) {
        return false;
      }
      final ConsumerId other = (ConsumerId) obj;
      return this.id == other.id && Objects.equals(this.name, other.name);
    }

    @Override
    public String toString() {
      return "ConsumerId(" + name + ", " + id + ")";
    }
  }

  public static class Accessor extends KafkaAccessor {

    private static final long serialVersionUID = 1L;

    String descriptorId;
    int numPartitions = 1;

    // list of consumers by name with assigned partitions
    transient Map<String, ConsumerGroup> consumerGroups;
    // ingests in different partitions
    transient List<List<StreamElement>> written;
    // (consumer name, consumer id) -> list(partition id, offset) (sparse)
    transient Map<ConsumerId, Map<Integer, Integer>> consumerOffsets;
    // (consumer name, partition id) -> committed offset
    transient Map<Pair<String, Integer>, AtomicInteger> committedOffsets;

    Accessor(Accessor copy, Map<String, Object> cfg) {
      super(copy.getEntityDescriptor(), copy.getUri(), cfg);
      this.descriptorId = copy.descriptorId;
      this.numPartitions = copy.numPartitions;
      this.consumerGroups = copy.consumerGroups;
      this.written = copy.written;
      this.consumerOffsets = copy.consumerOffsets;
      this.committedOffsets = copy.committedOffsets;
      configure(copy.getUri(), cfg);
    }

    public Accessor(
        EntityDescriptor entity, URI uri, Map<String, Object> cfg, String descriptorId) {

      super(entity, uri, cfg);

      this.descriptorId = descriptorId;
      this.consumerOffsets = new ConcurrentHashMap<>();
      this.written = Collections.synchronizedList(new ArrayList<>());
      this.consumerGroups = new ConcurrentHashMap<>();
      this.committedOffsets = Collections.synchronizedMap(new HashMap<>());

      configure(uri, cfg);
    }

    private void configure(URI uri, Map<String, Object> cfg) {
      numPartitions =
          Optional.ofNullable(cfg.get(CFG_NUM_PARTITIONS))
              .filter(o -> o != null)
              .map(o -> Integer.valueOf(o.toString()))
              .orElse(numPartitions);

      for (int i = 0; i < numPartitions; i++) {
        written.add(Collections.synchronizedList(new ArrayList<>()));
      }

      @SuppressWarnings("unchecked")
      Class<ElementSerializer<?, ?>> cls =
          Optional.ofNullable(cfg.get(KafkaAccessor.SERIALIZER_CLASS))
              .map(Object::toString)
              .map(c -> (Class) Classpath.findClass(c, ElementSerializer.class))
              .orElse(serializerClass);

      serializerClass = cls;

      log.info("Created accessor with URI {} and {} partitions", uri, numPartitions);
    }

    @Override
    public <K, V> KafkaConsumerFactory<K, V> createConsumerFactory() {

      ElementSerializer<K, V> serializer = getSerializer();
      return new KafkaConsumerFactory<K, V>(
          getUri(), new Properties(), serializer.keySerde(), serializer.valueSerde()) {

        @Override
        public KafkaConsumer<K, V> create() {
          return create(allPartitions());
        }

        @Override
        public KafkaConsumer<K, V> create(Collection<Partition> partitions) {
          String name = "unnamed-consumer-" + UUID.randomUUID().toString();
          ConsumerGroup group = new ConsumerGroup(name, getTopic(), numPartitions, false);
          return mockKafkaConsumer(name, group, serializer, partitions, null);
        }

        @Override
        public KafkaConsumer<K, V> create(String name) {
          return create(name, null);
        }

        @Override
        public KafkaConsumer<K, V> create(
            String name, @Nullable ConsumerRebalanceListener listener) {

          synchronized (LocalKafkaCommitLogDescriptor.class) {
            ConsumerGroup group = consumerGroups.get(name);
            if (group == null) {
              group = new ConsumerGroup(name, getTopic(), numPartitions, true);
              consumerGroups.put(name, group);
            }

            return mockKafkaConsumer(name, group, serializer, null, listener);
          }
        }

        private List<Partition> allPartitions() {
          List<Partition> ret = new ArrayList<>();
          for (int i = 0; i < numPartitions; i++) {
            int id = i;
            ret.add(Partition.of(id));
          }
          return ret;
        }
      };
    }

    /** Mock kafka consumer consuming from specified partitions. */
    @SuppressWarnings("unchecked")
    <K, V> KafkaConsumer<K, V> mockKafkaConsumer(
        String name,
        ConsumerGroup group,
        ElementSerializer<K, V> serializer,
        @Nullable Collection<Partition> assignedPartitions,
        @Nullable ConsumerRebalanceListener listener) {

      log.info(
          "Creating mock kafka consumer name {}, with committed offsets {}",
          name,
          committedOffsets);

      KafkaConsumer<K, V> mock = mock(KafkaConsumer.class);

      int assignedId =
          assignedPartitions != null ? group.add(assignedPartitions) : group.add(listener);
      final AtomicBoolean polled = new AtomicBoolean();

      ConsumerId consumerId = ConsumerId.of(name, assignedId);
      consumerOffsets.put(
          consumerId,
          group
              .getAssignment(consumerId.getId())
              .stream()
              .map(
                  p -> {
                    int off = getCommittedOffset(name, p.getId());
                    off = off >= 0 ? off : written.get(p.getId()).size();
                    return Pair.of(p.getId(), off);
                  })
              .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));

      doAnswer(
              invocation -> {
                polled.set(true);
                Duration sleep = (Duration) invocation.getArguments()[0];
                return pollConsumer(group, sleep.toMillis(), consumerId, serializer, listener);
              })
          .when(mock)
          .poll(any());

      doAnswer(
              invocation -> {
                Collection<TopicPartition> tp;
                tp = (Collection<TopicPartition>) invocation.getArguments()[0];
                return getEndOffsets(name, tp);
              })
          .when(mock)
          .endOffsets(any());

      doAnswer(
              invocation -> {
                Collection<TopicPartition> c;
                c = (Collection<TopicPartition>) invocation.getArguments()[0];
                Map<TopicPartition, Long> starts =
                    c.stream().collect(Collectors.toMap(i -> i, i -> 0L));
                log.debug("Consumer {} beginningOffsets {}: {}", name, c, starts);
                return starts;
              })
          .when(mock)
          .beginningOffsets(any());

      doAnswer(
              invocation -> {
                Collection<TopicPartition> parts;
                parts = (Collection<TopicPartition>) invocation.getArguments()[0];
                seekConsumerToBeginning(consumerId, parts);
                return null;
              })
          .when(mock)
          .seekToBeginning(any());

      doAnswer(
              invocation -> {
                TopicPartition tp = (TopicPartition) invocation.getArguments()[0];
                long offset = (long) invocation.getArguments()[1];
                seekConsumerTo(consumerId, tp.partition(), offset);
                return null;
              })
          .when(mock)
          .seek(any(), anyLong());

      doAnswer(
              invocation -> {
                Map<TopicPartition, OffsetAndMetadata> commitMap;
                commitMap = (Map<TopicPartition, OffsetAndMetadata>) invocation.getArguments()[0];
                commitConsumer(name, commitMap);
                return null;
              })
          .when(mock)
          .commitSync((Map<TopicPartition, OffsetAndMetadata>) any());

      doAnswer(
              invocation -> {
                Set<TopicPartition> parts = (Set<TopicPartition>) invocation.getArguments()[0];
                return parts
                    .stream()
                    .map(
                        tp -> {
                          int off = getCommittedOffset(name, tp.partition());
                          if (off >= 0) {
                            return Pair.of(tp, new OffsetAndMetadata(off));
                          }
                          return Pair.of(tp, null);
                        })
                    .filter(p -> p.getSecond() != null)
                    .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
              })
          .when(mock)
          .committed((Set<TopicPartition>) any());

      doAnswer(
              invocation ->
                  polled.get()
                      ? group
                          .getAssignment(consumerId.getId())
                          .stream()
                          .map(p -> new TopicPartition(getTopic(), p.getId()))
                          .collect(Collectors.toSet())
                      : Collections.emptySet())
          .when(mock)
          .assignment();

      when(mock.partitionsFor(eq(group.getTopic())))
          .thenReturn(
              IntStream.range(0, group.getNumPartitions())
                  .mapToObj(i -> new PartitionInfo(group.getTopic(), i, null, null, null))
                  .collect(Collectors.toList()));

      doAnswer(
              invocation -> {
                group.remove(consumerId.getId());
                return null;
              })
          .when(mock)
          .close();

      doAnswer(
              invocation -> {
                TopicPartition tp = (TopicPartition) invocation.getArguments()[0];
                return (long)
                    consumerOffsets
                        .get(consumerId)
                        .entrySet()
                        .stream()
                        .filter(e -> e.getKey() == tp.partition())
                        .findAny()
                        .map(Map.Entry::getValue)
                        .orElse(-1);
              })
          .when(mock)
          .position(any());

      return mock;
    }

    private int getCommittedOffset(String name, int partition) {
      AtomicInteger committed = committedOffsets.get(Pair.of(name, partition));
      if (committed != null) {
        return committed.get();
      }
      return -1;
    }

    private void commitConsumer(String name, Map<TopicPartition, OffsetAndMetadata> commitMap) {
      synchronized (committedOffsets) {
        commitMap
            .entrySet()
            .forEach(
                entry -> {
                  int partition = entry.getKey().partition();
                  long offset = entry.getValue().offset();
                  committedOffsets.compute(
                      Pair.of(name, partition),
                      (tmp, old) -> {
                        if (old == null) {
                          return new AtomicInteger((int) offset);
                        }
                        old.set((int) offset);
                        return old;
                      });
                });
      }
      log.debug(
          "Consumer {} committed offsets {}, offsets now {}", name, commitMap, committedOffsets);
    }

    private void seekConsumerTo(ConsumerId consumerId, int partition, long offset) {

      Preconditions.checkArgument(offset >= 0, "Cannot seek to negative offset %s", offset);

      Map<Integer, Integer> partOffsets;
      partOffsets = consumerOffsets.computeIfAbsent(consumerId, c -> new HashMap<>());
      partOffsets.put(partition, (int) offset);
      log.debug("Consumer {} seeked to offset {} in partition {}", consumerId, offset, partition);
    }

    private void seekConsumerToBeginning(ConsumerId consumerId, Collection<TopicPartition> parts) {

      consumerOffsets.put(
          consumerId,
          parts
              .stream()
              .map(tp -> Pair.of(tp.partition(), 0))
              .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));

      log.debug("Consumer {} seeked to beginning of {}", consumerId.getName(), parts);
    }

    private Map<TopicPartition, Long> getEndOffsets(String name, Collection<TopicPartition> tp) {

      Map<TopicPartition, Long> ends = new HashMap<>();
      for (TopicPartition p : tp) {
        ends.put(
            new TopicPartition(getTopic(), p.partition()),
            (long) written.get(p.partition()).size());
      }
      log.debug("Consumer {} endOffsets {}: {}", name, tp, ends);
      return ends;
    }

    private <K, V> ConsumerRecords<K, V> pollConsumer(
        ConsumerGroup group,
        long period,
        ConsumerId consumerId,
        ElementSerializer<K, V> serializer,
        @Nullable ConsumerRebalanceListener listener)
        throws InterruptedException {

      String name = consumerId.getName();
      if (!consumerId.isAssigned()) {
        log.debug(
            "Initializing consumer {} after first time poll with listener {}", name, listener);
        if (!group.rebalanceIfNeeded() && listener != null) {
          listener.onPartitionsAssigned(
              group
                  .getAssignment(consumerId.getId())
                  .stream()
                  .map(p -> new TopicPartition(getTopic(), p.getId()))
                  .collect(Collectors.toList()));
        }
        consumerId.setAssigned(true);
      }
      // need to sleep in order not to pollute the heap with
      // unnecessary objects
      // this is because mockito somehow creates lots of objects
      // when it is invoked too often
      log.debug("Sleeping {} ms before attempting to poll", period);
      Thread.sleep(period);

      Map<TopicPartition, List<ConsumerRecord<K, V>>> map;
      map = new HashMap<>();
      Collection<Partition> assignment =
          Lists.newArrayList(group.getAssignment(consumerId.getId()));
      Map<Integer, Integer> offsets = consumerOffsets.get(consumerId);

      if (log.isDebugEnabled()) {
        log.debug(
            "Polling consumerId {}.{} with assignment {} and offsets {}",
            descriptorId,
            consumerId,
            assignment.stream().map(Partition::getId).collect(Collectors.toList()),
            offsets);
      }
      int maxToPoll = getMaxPollRecords();
      for (Partition part : assignment) {
        int partition = part.getId();
        List<StreamElement> partitionData = written.get(partition);
        int last = partitionData.size();
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        int off =
            offsets
                .entrySet()
                .stream()
                .filter(e -> e.getKey() == partition)
                .map(Map.Entry::getValue)
                .findAny()
                .orElse(getCommittedOffset(name, part.getId()));
        log.trace("Partition {} has last {}, reading from {}", partition, last, off);

        while (off < last && maxToPoll-- > 0) {
          if (off >= 0) {
            records.add(toConsumerRecord(partitionData.get(off), serializer, part.getId(), off));
          }
          off++;
        }

        // advance the offset
        offsets.put(partition, off);
        log.trace(
            "Advanced offset of consumer ID {} on partition {} to {}", consumerId, partition, off);
        if (!records.isEmpty()) {
          map.put(new TopicPartition(getTopic(), partition), records);
        }
      }
      log.debug("Consumer {} id {} polled records {}", name, consumerId, map);

      return new ConsumerRecords<>(map);
    }

    private <K, V> ConsumerRecord<K, V> toConsumerRecord(
        StreamElement ingest, ElementSerializer<K, V> serializer, int partitionId, int offset) {

      Pair<K, V> elem = serializer.write(ingest);
      int keyLength =
          serializer.keySerde().serializer().serialize(getTopic(), elem.getFirst()).length;
      int valueLength =
          ingest.isDelete()
              ? 0
              : serializer.valueSerde().serializer().serialize(getTopic(), elem.getSecond()).length;

      return new ConsumerRecord<>(
          getTopic(),
          partitionId,
          offset,
          ingest.getStamp(),
          TimestampType.CREATE_TIME,
          0L,
          keyLength,
          valueLength,
          elem.getFirst(),
          elem.getSecond());
    }

    public boolean allConsumed() {
      return allConsumed(written.stream().map(List::size).collect(Collectors.toList()));
    }

    public boolean allConsumed(List<Integer> untilOffsets) {
      return consumerGroups
          .keySet()
          .stream()
          .map(
              name -> {
                int partition = 0;
                for (int written : untilOffsets) {
                  int current = partition;
                  if (consumerOffsets
                      .get(ConsumerId.of(name, current))
                      .entrySet()
                      .stream()
                      .filter(e -> e.getKey() == current)
                      .anyMatch(e -> e.getValue() < written)) {
                    return false;
                  }
                  partition++;
                }
                return true;
              })
          .reduce(true, (a, b) -> a && b);
    }

    @Override
    LocalKafkaWriter newWriter() {
      return new LocalKafkaWriter(this, numPartitions, descriptorId);
    }

    @Override
    LocalKafkaLogReader newReader(Context context) {
      return new LocalKafkaLogReader(this, context);
    }

    // serialization
    // this is magic, don't waste your time to tackle it :-)
    private void writeObject(ObjectOutputStream oos) throws IOException {
      // default serialization
      oos.defaultWriteObject();
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
      // default deserialization
      ois.defaultReadObject();
      Accessor original = ACCESSORS.get(this.descriptorId).get(getUri());
      this.committedOffsets = original.committedOffsets;
      this.consumerGroups = original.consumerGroups;
      this.consumerOffsets = original.consumerOffsets;
      this.written = original.written;
    }
  }

  @Slf4j
  public static class LocalKafkaLogReader extends KafkaLogReader {

    private KafkaConsumer<Object, Object> consumer = null;

    public LocalKafkaLogReader(KafkaAccessor accessor, Context context) {
      super(accessor, context);
    }

    @Override
    public ObserveHandle observePartitions(
        String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      ObserveHandle ret =
          super.observePartitions(name, partitions, position, stopAtCurrent, observer);
      log.debug(
          "Started to observe partitions {} of LocalKafkaCommitLog URI {}", partitions, getUri());
      return ret;
    }

    @Override
    public ObserveHandle observe(String name, Position position, LogObserver observer) {

      ObserveHandle ret = super.observe(name, position, observer);
      log.debug(
          "Started to observe LocalKafkaCommitLog with URI {} by consumer {}", getUri(), name);
      return ret;
    }

    @Override
    ObserveHandle observeKafka(
        @Nullable String name,
        Collection<Partition> partitions,
        Position position,
        boolean stopAtCurrent,
        LogObserver observer) {

      ObserveHandle ret = super.observeKafka(name, partitions, position, stopAtCurrent, observer);
      log.debug(
          "Started to observe partitions {} of LocalKafkaCommitLog " + "with URI {} by consumer {}",
          partitions,
          getUri(),
          name);
      return ret;
    }

    @Override
    public ObserveHandle observeBulk(String name, Position position, LogObserver observer) {

      ObserveHandle ret = super.observeBulk(name, position, observer);
      log.debug("Started to bulk observe LocalKafkaCommitLog with URI {} by {}", getUri(), name);
      return ret;
    }

    @Override
    public Accessor getAccessor() {
      return (Accessor) accessor;
    }

    @VisibleForTesting
    KafkaConsumer<Object, Object> getConsumer() {
      return Objects.requireNonNull(consumer);
    }

    @Override
    KafkaConsumer<Object, Object> createConsumer(
        String name,
        Collection<Offset> offsets,
        ConsumerRebalanceListener listener,
        Position position) {

      return consumer = super.createConsumer(name, offsets, listener, position);
    }

    @Override
    public Factory<?> asFactory() {
      final KafkaAccessor accessor = getAccessor();
      final Context context = getContext();
      return repo -> new LocalKafkaLogReader(accessor, context);
    }
  }

  public static class LocalKafkaWriter extends KafkaWriter {

    private static final long serialVersionUID = 1L;

    private final int numPartitions;
    private final String descriptorId;

    public LocalKafkaWriter(
        LocalKafkaCommitLogDescriptor.Accessor accessor, int numPartitions, String descriptorId) {

      super(accessor);
      this.numPartitions = numPartitions;
      this.descriptorId = descriptorId;
    }

    @Override
    public void write(StreamElement data, CommitCallback callback) {
      int partitionId = accessor.getPartitioner().getPartitionId(data);
      int partition = (partitionId & Integer.MAX_VALUE) % numPartitions;
      Accessor local = (LocalKafkaCommitLogDescriptor.Accessor) accessor;
      local.written.get(partition).add(data);
      long offset = local.written.get(partition).size() - 1;
      log.debug(
          "Written data {} to LocalKafkaCommitLog descriptorId {} URI {}, "
              + "partition {} at offset {}",
          data,
          descriptorId,
          getUri(),
          partition,
          offset);

      callback.commit(true, null);
    }

    @Override
    public Accessor getAccessor() {
      return (Accessor) accessor;
    }

    @Override
    public OnlineAttributeWriter.Factory<?> asFactory() {
      final LocalKafkaCommitLogDescriptor.Accessor accessor = getAccessor();
      final int numPartitions = this.numPartitions;
      final String descriptorId = this.descriptorId;
      return repo -> new LocalKafkaWriter(accessor, numPartitions, descriptorId);
    }
  }

  private final String id = UUID.randomUUID().toString();
  private final Function<Accessor, Accessor> accessorModifier;

  public LocalKafkaCommitLogDescriptor() {
    this(Function.identity());
  }

  public LocalKafkaCommitLogDescriptor(Function<Accessor, Accessor> accessorModifier) {
    ACCESSORS.put(id, Collections.synchronizedMap(new HashMap<>()));
    this.accessorModifier = accessorModifier;
  }

  @Override
  public Accessor createAccessor(
      DirectDataOperator direct, EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

    Map<URI, Accessor> accessorsForId = ACCESSORS.get(id);
    return accessorsForId.computeIfAbsent(
        uri,
        u -> {
          Accessor newAccessor = new Accessor(entityDesc, u, cfg, id);
          return accessorModifier.apply(newAccessor);
        });
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("kafka-test") ? Accept.ACCEPT : Accept.REJECT;
  }
}
