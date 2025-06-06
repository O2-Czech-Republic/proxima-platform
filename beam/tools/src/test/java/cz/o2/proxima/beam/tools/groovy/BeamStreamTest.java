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
package cz.o2.proxima.beam.tools.groovy;

import static cz.o2.proxima.beam.tools.groovy.BeamStream.getCoder;
import static org.junit.Assert.*;

import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.io.PairCoder;
import cz.o2.proxima.beam.tools.groovy.BeamStream.BulkWriteDoFn;
import cz.o2.proxima.beam.tools.groovy.BeamStream.IntegrateDoFn;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.transaction.Response.Flags;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.core.util.SerializableScopedValue;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.collect.Iterables;
import cz.o2.proxima.internal.com.google.common.collect.Lists;
import cz.o2.proxima.tools.groovy.JavaTypedClosure;
import cz.o2.proxima.tools.groovy.Stream;
import cz.o2.proxima.tools.groovy.StreamTest;
import cz.o2.proxima.tools.groovy.TestStreamProvider;
import cz.o2.proxima.tools.groovy.WindowedStream;
import cz.o2.proxima.tools.groovy.util.Closures;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import groovy.lang.Closure;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Slf4j
@RunWith(Parameterized.class)
public class BeamStreamTest extends StreamTest {

  private final transient Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf"));
  transient BeamDataOperator op;

  @Parameters
  public static Collection<Boolean> parameters() {
    return Arrays.asList(false, true);
  }

  final boolean stream;

  public BeamStreamTest(boolean stream) {
    super(provider(stream));
    this.stream = stream;
  }

  @Before
  public void setUp() {
    op = repo.getOrCreateOperator(BeamDataOperator.class);
  }

  @After
  public void tearDown() {
    op.close();
  }

  static TestStreamProvider provider(boolean stream) {
    return provider(stream, DirectRunner.class);
  }

  static TestStreamProvider provider(boolean stream, Class<? extends PipelineRunner<?>> runner) {
    return new TestStreamProvider() {
      @SuppressWarnings("unchecked")
      @Override
      public <T> Stream<T> of(List<T> values) {
        Set<Class<?>> classes = values.stream().map(Object::getClass).collect(Collectors.toSet());

        Preconditions.checkArgument(
            classes.size() == 1, "Please pass uniform object types, got " + classes);

        TypeDescriptor<T> typeDesc = TypeDescriptor.of((Class) Iterables.getOnlyElement(classes));

        return injectTypeOf(
            new BeamStream<>(
                StreamConfig.empty(),
                true,
                registeringTypes(
                    PCollectionProvider.boundedOrUnbounded(
                        p -> p.apply(Create.of(values)).setTypeDescriptor(typeDesc),
                        p -> p.apply(asTestStream(values)).setTypeDescriptor(typeDesc),
                        stream)),
                WindowingStrategy.globalDefault(),
                () -> {
                  LockSupport.park();
                  return false;
                },
                () -> {
                  Pipeline p = BeamStream.createPipelineDefault();
                  p.getOptions().setRunner(runner);
                  return p;
                }));
      }
    };
  }

  private static <T> PCollectionProvider<T> registeringTypes(PCollectionProvider<T> inner) {
    return new PCollectionProvider<T>() {
      @Override
      public PCollection<T> materialize(Pipeline pipeline) {
        pipeline
            .getCoderRegistry()
            .registerCoderForClass(GlobalWindow.class, GlobalWindow.Coder.INSTANCE);
        pipeline
            .getCoderRegistry()
            .registerCoderForClass(IntervalWindow.class, IntervalWindowCoder.of());
        return inner.materialize(pipeline);
      }

      @Override
      public void asUnbounded() {
        inner.asUnbounded();
      }
    };
  }

  static <T> TestStream<T> asTestStream(List<T> values) {
    TestStream.Builder<T> builder = TestStream.create(KryoCoder.of());
    for (T val : values) {
      builder = builder.addElements(val);
    }
    return builder.advanceWatermarkToInfinity();
  }

  static <T> BeamStream<T> injectTypeOf(BeamStream<T> delegate) {
    return new BeamStream<T>(
        StreamConfig.empty(),
        delegate.isBounded(),
        delegate.collection,
        WindowingStrategy.globalDefault(),
        delegate.terminateCheck) {

      @SuppressWarnings("unchecked")
      @Override
      <T> Coder<T> coderOf(Pipeline pipeline, Closure<T> closure) {
        try {
          return getTypeOf(closure)
              .map(type -> getCoder(pipeline, type))
              .orElseGet(() -> super.coderOf(pipeline, closure));
        } catch (IllegalStateException ex) {
          log.debug("Error fetching coder for {}", closure, ex);
          return (Coder) getCoder(pipeline, TypeDescriptor.of(Object.class));
        }
      }

      @Override
      <X> BeamWindowedStream<X> windowed(
          Function<Pipeline, PCollection<X>> factory, WindowFn<? super X, ?> window) {

        return injectTypeOf(super.windowed(factory, window));
      }

      @Override
      <X> BeamStream<X> descendant(Function<Pipeline, PCollection<X>> factory) {
        return injectTypeOf(super.descendant(factory));
      }
    };
  }

  static <T> BeamWindowedStream<T> injectTypeOf(BeamWindowedStream<T> delegate) {
    return new BeamWindowedStream<T>(
        StreamConfig.empty(),
        delegate.isBounded(),
        delegate.getCollection(),
        delegate.getWindowingStrategy(),
        delegate.getTerminateCheck(),
        delegate.getPipelineFactory()) {

      @SuppressWarnings("unchecked")
      @Override
      <T> Coder<T> coderOf(Pipeline pipeline, Closure<T> closure) {
        try {
          return getTypeOf(closure)
              .map(type -> getCoder(pipeline, type))
              .orElseGet(() -> super.coderOf(pipeline, closure));
        } catch (IllegalStateException ex) {
          log.debug("Error fetching coder for {}", closure, ex);
          return (Coder) getCoder(pipeline, TypeDescriptor.of(Object.class));
        }
      }

      @Override
      <X> BeamWindowedStream<X> windowed(
          Function<Pipeline, PCollection<X>> factory, WindowFn<? super X, ?> window) {

        return injectTypeOf(super.windowed(factory, window));
      }

      @Override
      <X> BeamWindowedStream<X> descendant(Function<Pipeline, PCollection<X>> factory) {
        return injectTypeOf(super.descendant(factory));
      }
    };
  }

  @SuppressWarnings("unchecked")
  static <T> Optional<TypeDescriptor<T>> getTypeOf(Closure<T> closure) {
    if (closure instanceof JavaTypedClosure) {
      return Optional.of(TypeDescriptor.of(((JavaTypedClosure) closure).getType()));
    }
    return Optional.empty();
  }

  @Test(timeout = 10000)
  public void testInterruptible() throws InterruptedException {
    EntityDescriptor gateway = repo.getEntity("gateway");
    AttributeDescriptor<?> armed = gateway.getAttribute("armed");
    SynchronousQueue<Boolean> interrupt = new SynchronousQueue<>();
    Stream<StreamElement> stream =
        BeamStream.stream(
            op,
            Position.OLDEST,
            false,
            true,
            interrupt::take,
            BeamStream::createPipelineDefault,
            armed);
    CountDownLatch latch = new CountDownLatch(1);
    new Thread(
            () -> {
              // collect endless stream
              stream.collect();
              latch.countDown();
            })
        .start();
    // terminate
    interrupt.put(true);
    // and wait until the pipeline terminates
    latch.await();
    // make sonar happy
    assertTrue(true);
  }

  @Test
  public void testIntegratePerKeyDoFn() {
    for (int r = 0; r < 1; r++) {
      long now = System.currentTimeMillis();
      TestStream<Integer> test =
          TestStream.create(KryoCoder.<Integer>of())
              .addElements(
                  TimestampedValue.of(1, new Instant(now)),
                  TimestampedValue.of(2, new Instant(now - 1)),
                  TimestampedValue.of(3, new Instant(now - 2)))
              .advanceWatermarkTo(new Instant(now + 1000))
              .advanceWatermarkToInfinity();
      PipelineOptions opts = PipelineOptionsFactory.create();
      Pipeline pipeline = Pipeline.create(opts);
      PCollection<Integer> input = pipeline.apply(test);
      PCollection<Pair<Integer, Integer>> result =
          input
              .apply(
                  MapElements.into(
                          TypeDescriptors.kvs(
                              TypeDescriptors.integers(), TypeDescriptors.integers()))
                      .via(i -> KV.of(0, i)))
              .apply(
                  ParDo.of(
                      new IntegrateDoFn<>(
                          Integer::sum, k -> 0, KvCoder.of(VarIntCoder.of(), VarIntCoder.of()))))
              .setCoder(PairCoder.of(VarIntCoder.of(), VarIntCoder.of()));
      PAssert.that(result)
          .containsInAnyOrder(Arrays.asList(Pair.of(0, 3), Pair.of(0, 5), Pair.of(0, 6)));
      assertNotNull(pipeline.run());
    }
  }

  @Test
  public void testIntegratePerKeyDoFnWithStateBootstrap() {
    for (int r = 0; r < 1; r++) {
      long now = System.currentTimeMillis();
      TestStream<Integer> test =
          TestStream.create(KryoCoder.<Integer>of())
              .addElements(
                  TimestampedValue.of(1, new Instant(now)),
                  TimestampedValue.of(2, new Instant(now - 1)),
                  TimestampedValue.of(3, new Instant(now - 2)))
              .advanceWatermarkTo(new Instant(now + 1000))
              .advanceWatermarkToInfinity();
      PipelineOptions opts = PipelineOptionsFactory.create();
      Pipeline pipeline = Pipeline.create(opts);
      PCollection<Integer> input = pipeline.apply(test);
      PCollection<Pair<Integer, Integer>> result =
          input
              .apply(
                  MapElements.into(
                          TypeDescriptors.kvs(
                              TypeDescriptors.integers(), TypeDescriptors.integers()))
                      .via(i -> KV.of(i % 2, i)))
              .apply(
                  ParDo.of(
                      new IntegrateDoFn<>(
                          Integer::sum, k -> k, KvCoder.of(VarIntCoder.of(), VarIntCoder.of()))))
              .setCoder(PairCoder.of(VarIntCoder.of(), VarIntCoder.of()));
      PAssert.that(result)
          .containsInAnyOrder(Arrays.asList(Pair.of(0, 2), Pair.of(1, 4), Pair.of(1, 5)));
      assertNotNull(pipeline.run());
    }
  }

  @Test
  public void testUnionWithUnbounded() {
    Stream<Integer> stream = provider(true).of(Arrays.asList(1, 2, 3, 4));
    Stream<Integer> other = provider(this.stream).of(Arrays.asList(2, 3, 4, 5));
    @SuppressWarnings("unchecked")
    List<Double> collect =
        stream
            .union(other)
            .timeWindow(1000)
            .sum(Closures.from(this, arg -> Double.parseDouble(arg.toString())))
            .collect();
    assertEquals(1, collect.size());
    assertEquals(24.0, collect.get(0), 0.001);
  }

  @Test
  public void testIntegratePerKeyWithAllowedLateness() {
    Instant now = Instant.ofEpochMilli(1);
    TestStream<Integer> input =
        TestStream.create(VarIntCoder.of())
            .addElements(
                TimestampedValue.of(100, now.plus(100)),
                TimestampedValue.of(99, now.plus(99)),
                TimestampedValue.of(90, now.plus(90)))
            .advanceWatermarkTo(now.plus(50))
            // add late elements
            .addElements(
                TimestampedValue.of(10, now.plus(10)),
                TimestampedValue.of(9, now.plus(9)),
                TimestampedValue.of(1, now.plus(1)))
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<Integer> data = p.apply(input);
    BeamStream<Integer> stream = BeamStream.wrap(data);
    @SuppressWarnings("unchecked")
    List<Integer> result =
        stream
            .windowAll()
            .withAllowedLateness(30)
            .integratePerKey(
                Closures.from(this, tmp -> 1),
                Closures.from(this, a -> a),
                Closures.from(this, tmp -> 0),
                Closures.from(this, (a, b) -> (int) a + (int) b))
            .map(Closures.fromArray(this, args -> ((Pair<Integer, Integer>) args[0]).getSecond()))
            .collect();
    assertEquals(Arrays.asList(90, 189, 289), result);
  }

  @Test
  public void testExtractEarlyEmitting() {
    Duration extracted =
        BeamStream.extractEarlyEmitting(
            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1)));
    assertEquals(1, extracted.getStandardSeconds());
    extracted =
        BeamStream.extractEarlyEmitting(
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(1))));
    assertEquals(1, extracted.getStandardSeconds());
  }

  @Test
  public void testReduceValueStateByKeyWithEarlyEmitting() {
    Instant now = Instant.ofEpochMilli(1);
    TestStream<Integer> input =
        TestStream.create(VarIntCoder.of())
            .addElements(
                TimestampedValue.of(100, now.plus(100)),
                TimestampedValue.of(99, now.plus(99)),
                TimestampedValue.of(90, now.plus(90)))
            .advanceWatermarkTo(now.plus(85))
            .advanceWatermarkTo(now.plus(90))
            .advanceWatermarkTo(now.plus(95))
            .advanceWatermarkTo(now.plus(100))
            .advanceWatermarkTo(now.plus(105))
            .addElements(TimestampedValue.of(110, now.plus(110)))
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<Integer> data = p.apply(input);
    BeamStream<Integer> stream = BeamStream.wrap(data);

    @SuppressWarnings("unchecked")
    List<Integer> result =
        stream
            .windowAll()
            .withEarlyEmitting(1)
            .reduceValueStateByKey(
                Closures.from(this, tmp -> 1),
                Closures.from(this, a -> a),
                Closures.from(this, tmp -> 0),
                Closures.from(this, (s, v) -> s),
                Closures.from(this, (s, v) -> v))
            .map(Closures.fromArray(this, args -> ((Pair<Integer, Integer>) args[0]).getSecond()))
            .collect();
    assertEquals(
        Arrays.asList(0, 90, 90, 99, 100, 100, 100, 110),
        result.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  public void testPeriodicStateFlushing() {
    if (!stream) {
      return;
    }
    Instant now = Instant.ofEpochMilli(1);
    TestStream<KV<String, Integer>> input =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
            .addElements(
                TimestampedValue.of(KV.of("a", 0), now.plus(1)),
                TimestampedValue.of(KV.of("b", 1), now.plus(2)),
                TimestampedValue.of(KV.of("a", 1), now.plus(10)),
                TimestampedValue.of(KV.of("b", 0), now.plus(15)))
            .advanceWatermarkTo(now.plus(1))
            .advanceWatermarkTo(now.plus(2))
            .advanceWatermarkTo(now.plus(5))
            .advanceWatermarkTo(now.plus(7))
            .advanceWatermarkTo(now.plus(10))
            .advanceWatermarkTo(now.plus(12))
            .advanceWatermarkTo(now.plus(15))
            .advanceWatermarkToInfinity();

    Pipeline p = Pipeline.create();
    PCollection<KV<String, Integer>> data = p.apply(input);
    BeamStream<KV<String, Integer>> stream = BeamStream.wrap(data);

    @SuppressWarnings("unchecked")
    List<Pair<Long, Long>> result =
        stream
            .windowAll()
            .withEarlyEmitting(1)
            .reduceValueStateByKey(
                Closures.from(this, kv -> ((KV) kv).getKey()),
                Closures.from(this, kv -> ((KV) kv).getValue()),
                Closures.from(this, tmp -> 0),
                Closures.from(this, (s, v) -> v == null ? s : v),
                Closures.from(this, (s, v) -> v))
            .timeWindow(1)
            .filter(Closures.from(this, pair -> ((Pair<String, Integer>) pair).getSecond() > 0))
            .map(Closures.from(this, pair -> ((Pair<String, Integer>) pair).getFirst()))
            .timeWindow(1)
            .distinct()
            .count()
            .withTimestamp()
            .collect();
    assertEquals(
        Lists.newArrayList(1L, 1L, 1L, 1L, 2L, 2L, 1L, 1L),
        result.stream()
            .sorted(Comparator.comparing(Pair::getSecond))
            .map(Pair::getFirst)
            .collect(Collectors.toList()));
  }

  @Test
  public void testPersistIntoTargetFamilySortedOnSpark() {
    if (stream) {
      return;
    }
    Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    EntityDescriptor event = repo.getEntity("event");
    AttributeDescriptor<byte[]> data = event.getAttribute("data");
    long now = System.currentTimeMillis();
    SparkRunner sparkRunner = SparkRunner.create();
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> elements =
        p.apply(
            Create.timestamped(
                timestamped(upsertRandom(event, data, now)),
                timestamped(upsertRandom(event, data, now - 1))));

    Collector<Pair<Long, StreamElement>> collected = new Collector<>();

    SerializableScopedValue<Integer, AttributeWriterBase> writer =
        new SerializableScopedValue<>(
            () -> collectingBulkWriter(collected).asFactory().apply(repo));

    BeamStream.BulkWriterFactory writerFactory =
        BeamStream.BulkWriterFactory.wrap(
            writer,
            java.util.stream.Stream.of(data)
                .map(AttributeDescriptor::getName)
                .collect(Collectors.toSet()));

    elements.apply(BeamStream.createBulkWriteTransform(tmp -> 1, new BulkWriteDoFn(writerFactory)));

    sparkRunner.run(p).waitUntilFinish();

    List<Long> stamps =
        collected.get().stream()
            .filter(pair -> pair.getSecond() != null)
            .map(Pair::getSecond)
            .map(StreamElement::getStamp)
            .collect(Collectors.toList());
    assertEquals(Lists.newArrayList(now - 1, now), stamps);
    List<Long> watermarks =
        collected.get().stream().map(Pair::getFirst).collect(Collectors.toList());
    assertEquals(
        Lists.newArrayList(now - 1, now, BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()),
        watermarks);
  }

  @Test
  public void testTransactionRejectedException() {
    Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    EntityDescriptor event = repo.getEntity("event");
    AttributeDescriptor<byte[]> data = event.getAttribute("data");
    long now = System.currentTimeMillis();
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> elements =
        p.apply(
            Create.timestamped(
                timestamped(upsertRandom(event, data, now)),
                timestamped(upsertRandom(event, data, now - 1))));

    SerializableScopedValue<Integer, List<StreamElement>> output =
        new SerializableScopedValue<>(() -> Collections.synchronizedList(Lists.newArrayList()));
    SerializableScopedValue<Integer, OnlineAttributeWriter> writer =
        new SerializableScopedValue<>(() -> rejectingOnlineWriter(output.get(0)));

    BeamStream.wrap(elements).writeUsingOnlineWriterFactory("name", w -> writer.get(0));
    assertEquals(2, output.get(0).size());
    assertEquals(
        Lists.newArrayList(now - 1, now),
        output.get(0).stream().map(StreamElement::getStamp).sorted().collect(Collectors.toList()));
  }

  @Test
  public void testFieldClassExtraction() {
    Set<Class<?>> classes =
        BeamStream.fieldsRecursively(new MyTestedExtractedClass()).collect(Collectors.toSet());
    assertTrue(
        classes.containsAll(
            Arrays.asList(
                ParentClass.class,
                MyTestedExtractedClass.class,
                MyTestedExtractedClass.NestedStaticClass.class,
                MyTestedExtractedClass.NestedNonStaticClass.class,
                Long.class,
                String.class,
                Float.class,
                Integer.class,
                Number.class,
                List.class,
                ArrayList.class)));
  }

  @Test
  public void testExceptionRethrow() {
    expectThrow(() -> BeamStream.rethrow(new OutOfMemoryError()), OutOfMemoryError.class);
    expectThrow(() -> BeamStream.rethrow(new RuntimeException("exc")), RuntimeException.class);
    expectThrow(() -> BeamStream.rethrow(new IOException()), IllegalStateException.class);
  }

  @Test
  public void testImpulse() {
    try {
      WindowedStream<Integer> impulse = BeamStream.impulse(null, op, Pipeline::create, () -> 1);
      List<Pair<String, Integer>> result =
          impulse
              .combine(
                  Closures.from(this, ign -> ""),
                  0,
                  Closures.from(this, (a, b) -> (int) a + (int) b))
              .collect();
      assertEquals(Collections.singletonList(Pair.of("", 1)), result);
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw ex;
    }
  }

  @Test(timeout = 60000)
  public void testPeriodicImpulse() throws InterruptedException {
    SerializableScopedValue<Integer, AtomicBoolean> finished =
        new SerializableScopedValue<>(AtomicBoolean::new);
    SerializableScopedValue<Integer, AtomicInteger> seen =
        new SerializableScopedValue<>(AtomicInteger::new);
    SerializableScopedValue<Integer, AtomicInteger> emitted =
        new SerializableScopedValue<>(AtomicInteger::new);
    SerializableScopedValue<Integer, CountDownLatch> latch =
        new SerializableScopedValue<>(() -> new CountDownLatch(3));
    WindowedStream<Integer> impulse =
        BeamStream.periodicImpulse(
            null,
            op,
            () -> {
              PipelineOptions opts = PipelineOptionsFactory.create();
              opts.as(DirectOptions.class).setBlockOnRun(false);
              return Pipeline.create(opts);
            },
            () -> {
              latch.get(0).countDown();
              return emitted.get(0).incrementAndGet();
            },
            1000,
            () -> finished.get(0).get());

    Thread pipeline =
        new Thread(
            () -> {
              try {
                impulse
                    .map(
                        Closures.from(
                            this,
                            el -> {
                              if (seen.get(0).incrementAndGet() > 3) {
                                finished.get(0).set(true);
                              }
                              return el;
                            }))
                    .collect();
              } catch (Exception ex) {
                if (ex.getCause() != null && ExceptionUtils.isInterrupted(ex.getCause())) {
                  // pass
                }
                throw ex;
              }
            });
    pipeline.setDaemon(true);
    pipeline.start();
    latch.get(0).await();
    // make sonar happy
    assertTrue(true);
  }

  @Test
  public void testWrapWithRepository() {
    Pipeline p = Pipeline.create();
    PCollection<byte[]> input = p.apply(Impulse.create());
    Stream<byte[]> stream = BeamStreamProvider.wrap(repo, input);
    List<ByteBuffer> result =
        stream.collect().stream().map(ByteBuffer::wrap).collect(Collectors.toList());
    assertEquals(Collections.singletonList(ByteBuffer.wrap(new byte[] {})), result);
  }

  @Test
  public void testMissingProviderForBatchUpdates() {
    EntityDescriptor event = repo.getEntity("event");
    AttributeDescriptor<?> data = event.getAttribute("data");
    try (DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class)) {
      OnlineAttributeWriter writer = Optionals.get(direct.getWriter(data));
      writer.write(
          StreamElement.upsert(
              event,
              data,
              UUID.randomUUID().toString(),
              "key",
              data.getName(),
              System.currentTimeMillis(),
              new byte[] {}),
          CommitCallback.noop());
      WindowedStream<StreamElement> input =
          BeamStream.batchUpdates(
              op,
              Watermarks.MIN_WATERMARK,
              Watermarks.MAX_WATERMARK,
              () -> false,
              () -> {
                PipelineOptions opts = PipelineOptionsFactory.create();
                opts.setRunner(FlinkRunner.class);
                return Pipeline.create(opts);
              },
              Collections.singletonList(data).toArray(new AttributeDescriptor[] {}));
      WindowedStream<Object> mapped =
          input.map(
              Closures.from(
                  this,
                  () -> {
                    throw new RuntimeException("Fail!");
                  }));
      try {
        List<Object> result = mapped.collect();
        fail("Should have thrown exception");
      } catch (IllegalArgumentException ex) {
        assertTrue(
            ex.getMessage(),
            ex.getMessage().startsWith("Cannot find suitable family for attributes ["));
      }
    }
  }

  private void expectThrow(Runnable r, Class<? extends Throwable> errorClass) {
    try {
      r.run();
    } catch (Throwable e) {
      assertTrue(errorClass.isAssignableFrom(e.getClass()));
    }
  }

  private StreamElement upsertRandom(
      EntityDescriptor event, AttributeDescriptor<byte[]> data, long now) {

    String key = UUID.randomUUID().toString();
    return StreamElement.upsert(event, data, key, key, data.getName(), now, new byte[] {0});
  }

  private static TimestampedValue<StreamElement> timestamped(StreamElement element) {
    return TimestampedValue.of(element, Instant.ofEpochMilli(element.getStamp()));
  }

  private static class ParentClass {
    private Long longField;
  }

  private static class MyTestedExtractedClass extends ParentClass {
    private static Double staticField;

    private static class NestedStaticClass {
      public String stringVal;
    }

    private class NestedNonStaticClass {
      protected Float floatVal;
    }

    private MyTestedExtractedClass recursive;
    private Integer intVal;
    private NestedNonStaticClass nonStaticInner;
    private NestedStaticClass staticInner;
    private int primitiveInt;
    private List<Object> list = new ArrayList<>();
  }

  private static BulkAttributeWriter collectingBulkWriter(
      Collector<Pair<Long, StreamElement>> collected) {

    return new BulkAttributeWriter() {
      @Override
      public void write(StreamElement data, long watermark, CommitCallback statusCallback) {
        collected.add(Pair.of(watermark, data));
      }

      @Override
      public void updateWatermark(long watermark) {
        collected.add(Pair.of(watermark, null));
      }

      @Override
      public Factory<?> asFactory() {
        return repo -> collectingBulkWriter(collected);
      }

      @Override
      public URI getUri() {
        return URI.create("fake:///");
      }

      @Override
      public void rollback() {}

      @Override
      public void close() {}
    };
  }

  private static OnlineAttributeWriter rejectingOnlineWriter(List<StreamElement> written) {
    return new OnlineAttributeWriter() {
      AtomicInteger attempt = new AtomicInteger(0);

      @Override
      public void write(StreamElement data, CommitCallback statusCallback) {
        if (attempt.getAndIncrement() < 1) {
          statusCallback.commit(
              false, new TransactionRejectedException("transaction", Flags.ABORTED) {});
        } else {
          written.add(data);
          statusCallback.commit(true, null);
        }
      }

      @Override
      public Factory<? extends OnlineAttributeWriter> asFactory() {
        return input -> rejectingOnlineWriter(written);
      }

      @Override
      public URI getUri() {
        return URI.create("fake:///");
      }

      @Override
      public void close() {}
    };
  }

  private static class Collector<T> implements Serializable {

    private static final Map<Integer, List<?>> MAP = new ConcurrentHashMap<>();

    final int id = System.identityHashCode(this);

    void add(T what) {
      @SuppressWarnings("unchecked")
      List<T> list =
          (List) MAP.computeIfAbsent(id, k -> Collections.synchronizedList(new ArrayList<>()));
      list.add(what);
    }

    @SuppressWarnings("unchecked")
    List<T> get() {
      return (List) Optional.ofNullable(MAP.get(id)).orElse(new ArrayList<>());
    }
  }
}
