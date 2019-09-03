/**
 * Copyright 2017-${Year} O2 Czech Republic, a.s.
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

import static org.junit.Assert.*;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.io.PairCoder;
import cz.o2.proxima.beam.tools.groovy.BeamStream.IntegrateDoFn;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.tools.groovy.JavaTypedClosure;
import cz.o2.proxima.tools.groovy.Stream;
import cz.o2.proxima.tools.groovy.StreamTest;
import cz.o2.proxima.tools.groovy.TestStreamProvider;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Slf4j
@RunWith(Parameterized.class)
public class BeamStreamTest extends StreamTest {

  @Parameters
  public static Collection<Boolean> parameters() {
    return Arrays.asList(false, true);
  }

  final boolean stream;

  public BeamStreamTest(boolean stream) {
    super(provider(stream));
    this.stream = stream;
  }

  static TestStreamProvider provider(boolean stream) {
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
                PCollectionProvider.boundedOrUnbounded(
                    p -> p.apply(Create.of(values)).setTypeDescriptor(typeDesc),
                    p -> p.apply(asTestStream(values)).setTypeDescriptor(typeDesc),
                    stream),
                WindowingStrategy.globalDefault(),
                () -> {
                  LockSupport.park();
                  return false;
                }));
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
        delegate.collection,
        delegate.getWindowingStrategy(),
        delegate.terminateCheck,
        delegate.pipelineFactory) {

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
    Repository repo = Repository.of(() -> ConfigFactory.load("test-reference.conf"));
    BeamDataOperator op = repo.asDataOperator(BeamDataOperator.class);
    EntityDescriptor gateway =
        repo.findEntity("gateway").orElseThrow(() -> new IllegalStateException("Missing gateway"));
    AttributeDescriptor<?> armed =
        gateway
            .findAttribute("armed")
            .orElseThrow(() -> new IllegalStateException("Missing armed"));
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
      PCollection<KV<Integer, Integer>> kvs =
          MapElements.of(input)
              .using(
                  i -> KV.of(0, i),
                  TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.integers()))
              .output();
      PCollection<Pair<Integer, Integer>> result =
          kvs.apply(
                  ParDo.of(
                      new IntegrateDoFn<>(
                          (a, b) -> a + b,
                          k -> 0,
                          KvCoder.of(VarIntCoder.of(), VarIntCoder.of()),
                          10)))
              .setCoder(PairCoder.of(VarIntCoder.of(), VarIntCoder.of()));
      PAssert.that(result).containsInAnyOrder(Pair.of(0, 3), Pair.of(0, 5), Pair.of(0, 6));
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
      PCollection<KV<Integer, Integer>> kvs =
          MapElements.of(input)
              .using(
                  i -> KV.of(i % 2, i),
                  TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.integers()))
              .output();
      PCollection<Pair<Integer, Integer>> result =
          kvs.apply(
                  ParDo.of(
                      new IntegrateDoFn<>(
                          (a, b) -> a + b,
                          k -> k,
                          KvCoder.of(VarIntCoder.of(), VarIntCoder.of()),
                          10)))
              .setCoder(PairCoder.of(VarIntCoder.of(), VarIntCoder.of()));
      PAssert.that(result).containsInAnyOrder(Pair.of(0, 2), Pair.of(1, 4), Pair.of(1, 5));
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
            .sum(
                new Closure<Double>(this) {
                  @Override
                  public Double call(Object arg) {
                    return Double.valueOf(arg.toString());
                  }
                })
            .collect();
    assertEquals(1, collect.size());
    assertEquals(24.0, collect.get(0), 0.001);
  }
}
