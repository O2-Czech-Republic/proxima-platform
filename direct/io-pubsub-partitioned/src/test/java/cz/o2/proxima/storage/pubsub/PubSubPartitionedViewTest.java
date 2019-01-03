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

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Iterables;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.pubsub.shaded.com.google.protobuf.ByteString;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.Partitioner;
import static cz.o2.proxima.storage.pubsub.PartitionedPubSubAccessor.CFG_NUM_PARTITIONS;
import static cz.o2.proxima.storage.pubsub.PartitionedPubSubAccessor.CFG_PARTITIONER;
import cz.o2.proxima.storage.pubsub.internal.proto.PubSub;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.beam.BeamExecutor;
import cz.seznam.euphoria.beam.BeamFlow;
import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.beam.window.BeamWindow;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeInterval;
import cz.seznam.euphoria.core.client.functional.VoidFunction;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.shadow.com.google.common.collect.Streams;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Test;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;

/**
 * Test suite for {@link PubSubPartitionedView}.
 */
public class PubSubPartitionedViewTest implements Serializable {

  private static final Map<Integer, Boolean> repartitionMap = new ConcurrentHashMap<>();
  private static final Map<Integer, String> stringContext = new ConcurrentHashMap<>();

  static {
    VoidFunction<Kryo> factory = KryoCoder.FACTORY;
    KryoCoder.withKryoFactory(() -> {
      Kryo ret = factory.apply();
      ret.setRegistrationRequired(true);
      ret.register(PubsubMessage.class);
      ret.register(Collections.emptyMap().getClass());
      ret.register(byte[].class);
      ret.register(AttributeData.class);
      ret.register(BeamWindow.class);
      ret.register(TimeInterval.class);
      return ret;
    });
  }

  private static class TestedPubSubPartitionedView extends PubSubPartitionedView {

    final EntityDescriptor entity;
    final transient TestStream<PubsubMessage> input;
    final long now;

    public TestedPubSubPartitionedView(
        PartitionedPubSubAccessor accessor, Context context,
        List<PubsubMessage> input,
        EntityDescriptor entity) {

      super(accessor, context);
      this.entity = entity;
      this.input = stream(input, now = System.currentTimeMillis());
    }

    public TestedPubSubPartitionedView(
        PartitionedPubSubAccessor accessor, Context context,
        long now, TestStream<PubsubMessage> stream,
        EntityDescriptor entity) {

      super(accessor, context);
      this.input = stream;
      this.now = now;
      this.entity = entity;
    }

    private static TestStream<PubsubMessage> stream(List<PubsubMessage> input, long now) {
      TestStream.Builder<PubsubMessage> create = TestStream.create(new KryoCoder<>());
      create = create.advanceWatermarkTo(new Instant(now));
      for (PubsubMessage e : input) {
        create = create.addElements(e);
      }
      return create
          .advanceWatermarkTo(new Instant(now + 86400000L))
          .advanceWatermarkToInfinity();
    }

    @Override
    Dataset<AttributeData> pubsubIO(
        BeamFlow flow, CommitLogReader reader, String name,
        long ign) {

      PCollection<PubsubMessage> in = flow.getPipeline().apply(input);
      return flow.wrapped(in.apply(ParDo.of(toData(entity)))
          .setCoder(new KryoCoder<>()));
    }

    private static DoFn<PubsubMessage, AttributeData> toData(
        EntityDescriptor entity) {

      return new DoFn<PubsubMessage, AttributeData>() {
        @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
        @ProcessElement
        public void process(ProcessContext context) {
          PubSubPartitionedView
              .toData(context.element())
              .ifPresent(context::output);
        }
      };
    }

  }

  public static class FirstBytePartitioner implements Partitioner {

    @Override
    public int getPartitionId(StreamElement element) {
      if (element.isDelete()) {
        return 0;
      }
      return element.getValue()[0];
    }

  }

  private final transient Repository repo = ConfigRepository.of(ConfigFactory.empty());
  private final AttributeDescriptorBase<byte[]> attr = AttributeDescriptor
      .newBuilder(repo)
      .setEntity("entity")
      .setName("attr")
      .setSchemeUri(new URI("bytes:///"))
      .build();

  private final EntityDescriptor entity = EntityDescriptor.newBuilder()
      .setName("entity")
      .addAttribute(attr)
      .build();

  public PubSubPartitionedViewTest() throws Exception {

  }

  @SafeVarargs
  final PubSubPartitionedView createView(
      Class<? extends Partitioner> partitionerClass,
      int numPartitions,
      TestStream<PubsubMessage> stream,
      long now,
      Pair<String, String>... cfg) {

    try {
      return new TestedPubSubPartitionedView(
          new PartitionedPubSubAccessor(
              entity, new URI("pgps://dummy/dummy"),
              Streams
                  .concat(
                      Stream
                          .of(
                              Pair.of(CFG_PARTITIONER, partitionerClass.getName()),
                              Pair.of(CFG_NUM_PARTITIONS, String.valueOf(numPartitions))),
                      Arrays.stream(cfg))
                  .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond))),
          context(), now, stream, entity);
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  @SafeVarargs
  final PubSubPartitionedView createView(
      Class<? extends Partitioner> partitionerClass,
      int numPartitions,
      List<PubsubMessage> messages,
      Pair<String, String>... cfg) {

    try {
      return new TestedPubSubPartitionedView(
          new PartitionedPubSubAccessor(
              entity, new URI("pgps://dummy/dummy"),
              Streams
                  .concat(
                      Stream
                          .of(
                              Pair.of(CFG_PARTITIONER, partitionerClass.getName()),
                              Pair.of(CFG_NUM_PARTITIONS, String.valueOf(numPartitions))),
                      Arrays.stream(cfg))
                  .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond))),
          context(), messages, entity);
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Before
  public void setUp() {
    PubSubPartitionedView.clearPartitioning();
    repartitionMap.clear();
  }

  @Test
  public void testViewNamedObserve() {
    PubSubPartitionedView view = createView(
        FirstBytePartitioner.class, 3,
        messages("key", System.currentTimeMillis(),
            new byte[] { 1, 2, 3 }, new byte[] { 2, 3, 4 }, new byte[] { 3, 4 }));

    testViewObserve(view, "dummy");
  }

  @Test
  public void testViewUnamedObserve() {
    PubSubPartitionedView view = createView(
        FirstBytePartitioner.class, 3,
        messages("key", System.currentTimeMillis(),
            new byte[] { 1, 2, 3 }, new byte[] { 2, 3, 4 }, new byte[] { 3, 4 }));

    testViewObserve(view, null);
  }

  @Test
  @Ignore(
      "This test fails for unknown reasons. "
          + "It seems related to coder used to store elements in BagState. "
          + "This suggest further debug is needed.")
  public void testViewUnamedObserveSorted() {
    long now = 1234567890000L;
    PubSubPartitionedView view = createView(
        FirstBytePartitioner.class, 1,
        stream("key",
            Triple.of(new byte[] { 3, 4 }, now + 200, now - 200),
            Triple.of(new byte[] { 2, 3, 4 }, now + 100, now - 150),
            Triple.of(new byte[] { 1, 2, 3 }, now, now + 500)),
        now,
        Pair.of(PartitionedPubSubAccessor.CFG_ORDERING_LATENESS, "500"),
        Pair.of(PartitionedPubSubAccessor.CFG_ORDERING_WINDOW, "500"));

    testViewObserveSorted(view, null);
  }

  @Test
  public void testViewPersist() {
    PubSubPartitionedView view = createView(
        FirstBytePartitioner.class, 3,
        messages("key", System.currentTimeMillis(),
            new byte[] { 1, 2, 3 }, new byte[] { 2, 3, 4 }, new byte[] { 3, 4 }));
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(DirectRunner.class);
    Pipeline pipeline = Pipeline.create(opts);
    BeamFlow flow = BeamFlow.create(pipeline);

    Dataset<Integer> output = createOutputDataset(view, flow, "dummy");
    ListDataSink<Integer> sink = ListDataSink.get();
    output.persist(sink);
    Executor executor = new BeamExecutor(opts);
    executor.submit(flow).join();
    assertEquals(1, sink.getOutputs().size());
    assertEquals(22, (int) sink.getOutputs().get(0));
  }

  public void testViewObserve(PartitionedView view, @Nullable String name) {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(DirectRunner.class);
    Pipeline pipeline = Pipeline.create(opts);
    BeamFlow flow = BeamFlow.create(pipeline);
    Dataset<Integer> ds = createOutputDataset(view, flow, name);

    PCollection<Integer> output = flow.unwrapped(ds);
    PAssert.that(output).containsInAnyOrder(22);
    pipeline.run();
  }

  public void testViewObserveSorted(PartitionedView view, @Nullable String name) {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(DirectRunner.class);
    Pipeline pipeline = Pipeline.create(opts);
    BeamFlow flow = BeamFlow.create(pipeline);
    Dataset<String> ds = createOutputDatasetSorted(view, flow, name);

    PCollection<String> output = flow.unwrapped(ds);
    PAssert.that(output).containsInAnyOrder(
        "[1, 2, 3]",
        "[1, 2, 3], [2, 3, 4]",
        "[1, 2, 3], [2, 3, 4], [3, 4]");
    pipeline.run();
  }


  Dataset<Integer> createOutputDataset(PartitionedView view, BeamFlow flow, String name) {
    Dataset<Integer> ds = view.observe(flow, name, new PartitionedLogObserver<Integer>() {

      @Override
      public boolean onNext(
          StreamElement ingest, PartitionedLogObserver.ConfirmCallback confirm,
          Partition partition, Consumer<Integer> collector) {

        assertTrue(
            "onRepartition has not been called on " + partition.getId(),
            repartitionMap.get(partition.getId()));
        assertEquals(ingest.getValue()[0] % 3, partition.getId());
        int sum = 0;
        for (byte b : ingest.getValue()) {
          sum += b;
        }
        collector.accept(sum);
        return true;
      }

      @Override
      public void onRepartition(Collection<Partition> assigned) {
        repartitionMap.put(Iterables.getOnlyElement(assigned).getId(), true);
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    });
    return ReduceWindow.of(ds)
        .combineBy(Sums.ofInts())
        .windowBy(Time.of(Duration.ofHours(1)))
        .output();
  }

  Dataset<String> createOutputDatasetSorted(
      PartitionedView view, BeamFlow flow, String name) {

    return view.observe(flow, name, new PartitionedLogObserver<String>() {

      @Override
      public boolean onNext(
          StreamElement ingest, PartitionedLogObserver.ConfirmCallback confirm,
          Partition partition, Consumer<String> collector) {

        assertTrue(
            "onRepartition has not been called on " + partition.getId(),
            repartitionMap.get(partition.getId()));
        String val = Arrays.toString(ingest.getValue());
        String res = stringContext.get(partition.getId());
        if (!res.isEmpty()) {
          res += ", " + val;
        } else {
          res = val;
        }
        stringContext.put(partition.getId(), res);
        collector.accept(res);
        return true;
      }

      @Override
      public void onRepartition(Collection<Partition> assigned) {
        stringContext.put(Iterables.getOnlyElement(assigned).getId(), "");
        repartitionMap.put(Iterables.getOnlyElement(assigned).getId(), true);
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    });
  }

  private List<PubsubMessage> messages(String key, long stamp, byte[]... payloads) {
    return Arrays.stream(payloads)
        .map(b -> toPubsub(key, stamp, b))
        .collect(Collectors.toList());
  }

  @SafeVarargs
  private final TestStream<PubsubMessage> stream(
      String key, Triple<byte[], Long, Long>... payloads) {

    TestStream.Builder<PubsubMessage> builder = TestStream.create(new KryoCoder<>());
    for (Triple<byte[], Long, Long> p : payloads) {
      TimestampedValue<PubsubMessage> value = TimestampedValue.of(
          toPubsub(key, p.getSecond(), p.getFirst()), new Instant(p.getSecond()));
      builder = builder.addElements(value);
      builder = builder.advanceWatermarkTo(new Instant(p.getThird()));
    }
    return builder.advanceWatermarkToInfinity();
  }

  PubsubMessage toPubsub(String key, long stamp, byte[] b) {
    return new PubsubMessage(PubSub.KeyValue.newBuilder()
        .setAttribute(attr.getName())
        .setKey(key)
        .setValue(ByteString.copyFrom(b))
        .setStamp(stamp)
        .build()
        .toByteArray(), Collections.emptyMap());
  }

  private Context context() {
    return new Context(() -> Executors.newCachedThreadPool()) {
    };
  }

}
