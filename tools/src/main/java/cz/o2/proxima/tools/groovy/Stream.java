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
package cz.o2.proxima.tools.groovy;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.tools.io.AttributeSink;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Session;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.operator.AssignEventTime;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.core.executor.Executor;
import groovy.lang.Closure;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;

/**
 * A stream abstraction with fluent style methods.
 */
@Slf4j
public class Stream<T> {

  public static <T> Stream<T> wrap(
      Executor executor, DatasetBuilder<T> dataset,
      Runnable terminatingOperationCall) {

    return new Stream<>(executor, dataset, terminatingOperationCall);
  }

  final Executor executor;
  final DatasetBuilder<T> dataset;
  final Runnable terminatingOperationCall;

  Stream(
      Executor executor,
      DatasetBuilder<T> dataset,
      Runnable terminatingOperationCall) {

    this.executor = executor;
    this.dataset = dataset;
    this.terminatingOperationCall = terminatingOperationCall;
  }

  @SuppressWarnings("unchecked")
  public <X> Stream<X> map(Closure<X> mapper) {
    Closure<X> dehydrated = mapper.dehydrate();
    return descendant(() ->
        MapElements.of(dataset.build())
            .using(e -> dehydrated.call(e))
            .output());
  }

  public Stream<T> filter(Closure<Boolean> predicate) {
    Closure<Boolean> dehydrated = predicate.dehydrate();
    return descendant(() ->
        Filter.of(dataset.build())
            .by(e -> dehydrated.call(e))
            .output());
  }

  public Stream<T> assignEventTime(Closure<Long> assigner) {
    Closure<Long> dehydrated = assigner.dehydrate();
    return descendant(() ->
        AssignEventTime.of(dataset.build())
            .using(e -> dehydrated.call(e))
            .output());
  }

  public Stream<Pair<Object, T>> withWindow() {
    return descendant(() ->
        FlatMap.of(dataset.build())
            .using((T in, Collector<Pair<Object, T>> ctx) -> {
              ctx.collect(Pair.of(ctx.getWindow(), in));
            })
            .output());
  }

  public void forEach(Closure<?> consumer) {
    Closure<?> dehydrated = consumer.dehydrate();
    Dataset<T> datasetBuilt = dataset.build();
    datasetBuilt.persist(new DataSink<T>() {
      @Override
      public Writer<T> openWriter(int i) {

        return new Writer<T>() {

          @Override
          public void write(T elem) throws IOException {
            dehydrated.call(elem);
          }

          @Override
          public void commit() throws IOException {
            // nop
          }

          @Override
          public void close() throws IOException {
            // nop
          }

        };
      }

      @Override
      public void commit() throws IOException {
        // nop
      }

      @Override
      public void rollback() throws IOException {
        // nop
      }

    });

    runFlow(datasetBuilt.getFlow());
  }

  private void runFlow(Flow flow) throws RuntimeException {
    try {
      executor.submit(flow).get();
    } catch (Exception ex) {
      log.error("Error in executing the flow", ex);
      throw new RuntimeException(ex);
    } finally {
      terminatingOperationCall.run();
    }
  }

  @SuppressWarnings("unchecked")
  public void persist(
      String host, int port,
      EntityDescriptor entity, AttributeDescriptor<T> attr,
      Closure<String> keyExtractor,
      Closure<T> valueExtractor,
      Closure<Long> timeExtractor) {

    Closure<String> keyDehydrated = keyExtractor.dehydrate();
    Closure<T> valueDehydrated = valueExtractor.dehydrate();
    Closure<Long> timeDehydrated = timeExtractor.dehydrate();
    Dataset<Triple<String, byte[], Long>> output;
    final ValueSerializer<T> serializer = attr.getValueSerializer();
    output = FlatMap.of((Dataset<Object>) dataset.build())
        .using((Object in, Collector<Triple<String, byte[], Long>> ctx) -> {
          String key = keyDehydrated.call(in);
          long stamp = timeDehydrated.call(in);
          byte[] value = serializer.serialize(valueDehydrated.call(in));
          ctx.collect(Triple.of(key, value, stamp));
        })
        .output();

    output.persist(new AttributeSink(host, port, entity, attr));

    runFlow(output.getFlow());
  }


  public TimeWindowedStream<T> timeWindow(long millis) {
    return new TimeWindowedStream<>(
        executor, dataset, millis, terminatingOperationCall);
  }

  public TimeWindowedStream<T> timeSlidingWindow(long millis, long slide) {
    return new TimeWindowedStream<>(
        executor, dataset, millis, slide, terminatingOperationCall);
  }

  @SuppressWarnings("unchecked")
  public WindowedStream<T, Session> sessionWindow(long gapDuration) {
    return new WindowedStream<>(
        executor, dataset,
        Session.of(Duration.ofMillis(gapDuration)),
        terminatingOperationCall,
        (w, d) -> w.earlyTriggering(d));
  }

  @SuppressWarnings("unchecked")
  public WindowedStream<T, GlobalWindowing> windowAll() {
    return new WindowedStream<>(
        executor, dataset, GlobalWindowing.get(),
        terminatingOperationCall,
        (w, d) -> {
          throw new UnsupportedOperationException("Euphoria issue #246");
        });
  }

  <X> Stream<X> descendant(DatasetBuilder<X> dataset) {
    return new Stream<>(executor, dataset, terminatingOperationCall);
  }

  public Stream<T> union(Stream<T> other) {
    return descendant(() -> {
      return Union.of(dataset.build(), other.dataset.build()).output();
    });
  }

}
