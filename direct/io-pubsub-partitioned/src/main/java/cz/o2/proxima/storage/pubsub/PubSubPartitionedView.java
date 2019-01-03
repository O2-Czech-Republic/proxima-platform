/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage.pubsub;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;

import static com.google.common.base.MoreObjects.firstNonNull;

import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.Partitioner;
import cz.o2.proxima.storage.pubsub.io.CommitLogSource;
import cz.o2.proxima.storage.pubsub.proto.PubSub;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.beam.BeamFlow;
import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.core.annotation.stability.Experimental;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

/**
 * A {@link PartitionedView} for Google PubSub.
 */
@Experimental
@Slf4j
class PubSubPartitionedView extends PubSubReader implements PartitionedView {

  private static final Set<Integer> repartitioned = Collections.synchronizedSet(
      new HashSet<>());

  @VisibleForTesting
  static void clearPartitioning() {
    repartitioned.clear();
  }

  private final transient PipelineOptions options;
  private final Partitioner partitioner;
  private final int numPartitions;
  private final Duration orderingLateness;
  private final Duration orderingWindow;

  PubSubPartitionedView(PartitionedPubSubAccessor accessor, Context context) {
    super(accessor, context);
    this.partitioner = accessor.getPartitioner();
    this.options = accessor.getOptions();
    this.numPartitions = accessor.getNumPartitions();
    this.orderingLateness = accessor.getOrderingLateness();
    this.orderingWindow = accessor.getOrderingWindow();
  }

  @Override
  public List<Partition> getPartitions() {
    return IntStream.range(0, numPartitions)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList());
  }

  @Override
  public <T> Dataset<T> observePartitions(
      Collection<Partition> partitions,
      PartitionedLogObserver<T> observer) {

    return observePartitions(BeamFlow.create(
        Pipeline.create(options)), partitions, observer);
  }

  @Override
  public <T> Dataset<T> observePartitions(
      Flow flow,
      Collection<Partition> partitions,
      PartitionedLogObserver<T> observer) {

    if (flow instanceof BeamFlow) {
      BeamFlow bf = (BeamFlow) flow;
      Dataset<AttributeData> msgs = pubsubIO(
          bf, this, flow.getName(), orderingLateness.toMillis());
      return applyObserver(
          getEntityDescriptor(),
          bf.unwrapped(msgs), partitioner,
          numPartitions, observer, orderingWindow,
          orderingLateness, bf);
    }
    throw new UnsupportedOperationException(
        "This view cooperates only with BeamFlow. Use euphoria-beam as executor "
      + "and construct flow by BeamFlow#create");
  }

  @Override
  public <T> Dataset<T> observe(String name, PartitionedLogObserver<T> observer) {
    return observe(
        BeamFlow.create(Pipeline.create(options)),
        name, observer);
  }

  @Override
  public <T> Dataset<T> observe(
      Flow flow, String name,
      PartitionedLogObserver<T> observer) {

    if (flow instanceof BeamFlow) {
      BeamFlow bf = (BeamFlow) flow;
      Dataset<AttributeData> msgs = pubsubIO(
          bf, this, name, orderingLateness.toMillis());

      return applyObserver(
          getEntityDescriptor(),
          bf.unwrapped(msgs), partitioner,
          numPartitions, observer, orderingWindow,
          orderingLateness, bf);
    }
    throw new UnsupportedOperationException(
        "This view cooperates only with BeamFlow. Use euphoria-beam as executor "
      + "and construct flow by BeamFlow#create");

  }

  private static <T> Dataset<T> applyObserver(
      EntityDescriptor entity,
      PCollection<AttributeData> msgs,
      Partitioner partitioner,
      int numPartitions,
      PartitionedLogObserver<T> observer,
      Duration orderingWindow,
      Duration orderingLateness,
      BeamFlow flow) {

    PCollection<KV<Integer, AttributeData>> parts = msgs
        .apply(MapElements
            .into(TypeDescriptors.kvs(
                TypeDescriptors.integers(),
                new TypeDescriptor<AttributeData>() { }))
            .via(input -> {
              int partition = toElement(entity, input)
                  .map(el -> (partitioner.getPartitionId(el) & Integer.MAX_VALUE)
                      % numPartitions)
                  .orElse(0);
              return KV.of(partition, input);
            }))
        .setCoder(KvCoder.of(VarIntCoder.of(), new KryoCoder<>()));

    final PCollection<T> result;
    if (!orderingLateness.isZero()) {
      result = parts.apply(ParDo.of(
          toTimedDoFn(entity, observer, orderingWindow, orderingLateness)));
    } else {
      result = parts.apply(ParDo.of(toUnwindowedDoFn(entity, observer)));
    }

    return flow.wrapped(result.setCoder(new KryoCoder<>()));
  }


  @VisibleForTesting
  Dataset<AttributeData> pubsubIO(
      BeamFlow flow,
      CommitLogReader reader, String name, long allowedLateness) {


    return flow.wrapped(flow.getPipeline()
        .apply(Read.from(CommitLogSource.of(reader, name, allowedLateness)))
        .setCoder(new KryoCoder<>()));

  }

  @VisibleForTesting
  static Optional<AttributeData> toData(PubsubMessage message) {
    try {
      PubSub.KeyValue parsed = PubSub.KeyValue.parseFrom(message.getPayload());
      return Optional.of(new AttributeData(
          parsed.getKey(),
          parsed.getAttribute(),
          parsed.getValue().toByteArray(),
          parsed.getDelete(),
          parsed.getDeleteWildcard(),
          parsed.getStamp()));
    } catch (InvalidProtocolBufferException ex) {
      log.warn("Failed to parse message {}", message, ex);
    }
    return Optional.empty();
  }

  private static <T> DoFn<KV<Integer, AttributeData>, T> toUnwindowedDoFn(
      EntityDescriptor entity,
      PartitionedLogObserver<T> observer) {

    return new DoFn<KV<Integer, AttributeData>, T>() {

      final @StateId("seq") StateSpec<ValueState<Void>> seq = StateSpecs.value();

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @ProcessElement
      public void process(
          ProcessContext context,
          @StateId("seq") ValueState<Void> ign) {

        int partitionId = context.element().getKey();
        if (repartitioned.add(partitionId)) {
          observer.onRepartition(Arrays.asList(() -> partitionId));
        }
        AttributeData data = context.element().getValue();
        toElement(entity, data)
            .ifPresent(elem -> observer.onNext(elem, (succ, exc) -> {
              if (!succ) {
                throw new RuntimeException(exc);
              }
            }, () -> partitionId, context::output));
      }
    };
  }


  private static class TimedDoFn<T> extends DoFn<KV<Integer, AttributeData>, T> {

    private final EntityDescriptor entity;
    private final PartitionedLogObserver<T> observer;
    private final Duration fireInterval;
    private final long allowedLatenessMs;

    TimedDoFn(
        EntityDescriptor entity,
        PartitionedLogObserver<T> observer,
        Duration fireInterval,
        Duration allowedLateness) {

      this.entity = entity;
      this.observer = observer;
      this.fireInterval = fireInterval;
      this.allowedLatenessMs = allowedLateness.toMillis();
    }


    @StateId("partition")
    final StateSpec<ValueState<Integer>> partitionSpec = StateSpecs.value();

    @StateId("heap")
    final StateSpec<BagState<AttributeData>> heapSpec = StateSpecs.bag(
        new KryoCoder<>());

    @StateId("watermark")
    final StateSpec<ValueState<Long>> watermark = StateSpecs.value();

    @TimerId("fire-event")
    final TimerSpec fireEventTimeSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @TimerId("fire-processing")
    final TimerSpec fireProcessingTimeSpec = TimerSpecs.timer(
        TimeDomain.PROCESSING_TIME);

    @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
    @ProcessElement
    public void process(
        ProcessContext context,
        @StateId("heap") BagState<AttributeData> heap,
        @StateId("partition") ValueState<Integer> partition,
        @TimerId("fire-event") Timer fire) {

      int partitionId = context.element().getKey();
      if (repartitioned.add(partitionId)) {
        observer.onRepartition(Arrays.asList(() -> partitionId));
        partition.write(partitionId);
        Instant now = Instant.now();
        fire.set(now);
        log.info(
            "Going to start processing time flushing after watermark reaches {}",
            now);
      }
      heap.add(context.element().getValue());
    }

    @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
    @OnTimer("fire-event")
    public synchronized void onTimerEvent(
        OnTimerContext context,
        @StateId("heap") BagState<AttributeData> heap,
        @StateId("watermark") ValueState<Long> watermark,
        @StateId("partition") ValueState<Integer> partition,
        @TimerId("fire-event") Timer fireEvent,
        @TimerId("fire-processing") Timer fireProcessing) {

      long timeBound = context.timestamp().getMillis() - allowedLatenessMs;
      flushSorted(
          heap.readLater(), watermark.readLater(), new Instant(timeBound),
          partition.readLater(), fireProcessing, context::output);
      fireEvent
          .offset(org.joda.time.Duration.millis(fireInterval.toMillis()))
          .setRelative();
    }

    @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
    @OnTimer("fire-processing")
    public synchronized void onTimerProcessing(
        OnTimerContext context,
        @StateId("heap") BagState<AttributeData> heap,
        @StateId("watermark") ValueState<Long> watermark,
        @StateId("partition") ValueState<Integer> partition,
        @TimerId("fire-processing") Timer fire) {

      long timeBound = System.currentTimeMillis() - allowedLatenessMs;
      flushSorted(
          heap.readLater(), watermark.readLater(), new Instant(timeBound),
          partition.readLater(), fire, context::output);
    }


    void flushSorted(
        BagState<AttributeData> heap,
        ValueState<Long> watermark,
        Instant timestamp,
        ValueState<Integer> partitionId,
        Timer fire,
        Consumer<T> consumer) {

      Iterable<AttributeData> elements = heap.read();
      List<AttributeData> toKeep = new ArrayList<>();
      List<AttributeData> toFlush = new ArrayList<>();
      elements.forEach(el -> {
        if (timestamp.isAfter(el.getStamp())) {
          toFlush.add(el);
        } else {
          toKeep.add(el);
        }
      });
      long lastWatermark = firstNonNull(watermark.read(), Long.MIN_VALUE);
      toFlush.stream()
          .sorted((a, b) -> Long.compare(a.getStamp(), b.getStamp()))
          .forEach(data -> {
            if (data.getStamp() < lastWatermark) {
              log.warn(
                  "Dropping after-watermark data {}, watermark is {}",
                  data, lastWatermark);
            } else {
              toElement(entity, data)
                  .ifPresent(el -> observer.onNext(el, (succ, exc) -> {
                    if (!succ) {
                      throw new RuntimeException(exc);
                    }
                  }, partitionId::read, consumer));
            }
          });
      if (!toFlush.isEmpty()) {
        watermark.write(timestamp.getMillis());
        heap.clear();
        toKeep.forEach(heap::add);
      }
      fire.offset(org.joda.time.Duration.millis(fireInterval.toMillis()))
          .setRelative();
    }

  }

  private static <T> DoFn<KV<Integer, AttributeData>, T> toTimedDoFn(
      EntityDescriptor entity,
      PartitionedLogObserver<T> observer,
      Duration fireInterval,
      Duration allowedLateness) {

    return new TimedDoFn<>(entity, observer, fireInterval, allowedLateness);
  }

  static Optional<StreamElement> toElement(
      EntityDescriptor entity, AttributeData data) {

    long stamp = data.getStamp();
    String uuid = UUID.randomUUID().toString();
    Optional<AttributeDescriptor<Object>> attr = entity.findAttribute(
        data.getAttribute());
    if (attr.isPresent()) {
      if (data.isDeleteWildcard()) {
        return Optional.of(StreamElement.deleteWildcard(
            entity, attr.get(), uuid, data.getKey(),
            data.getAttribute(), stamp));
      } else if (data.isDelete()) {
        return Optional.of(StreamElement.delete(
            entity, attr.get(), uuid, data.getKey(),
            data.getAttribute(), stamp));
      }
      return Optional.of(StreamElement.update(
          entity, attr.get(), uuid, data.getKey(), data.getAttribute(),
          stamp, data.getValue()));
    }
    log.warn("Missing attribute {} of entity {}", data.getAttribute(), entity);
    return Optional.empty();
  }

}
