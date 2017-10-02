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

package cz.o2.proxima.tools.groovy;

import cz.o2.proxima.tools.io.AttributeSink;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.core.executor.Executor;
import groovy.lang.Closure;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stream abstraction with fluent style methods.
 */
public class Stream<T> {

  private static final Logger LOG = LoggerFactory.getLogger(Stream.class);

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
  public <X> Stream<X> map(Closure<?> mapper) {
    Closure<?> dehydrated = mapper.dehydrate();
    return descendant(() ->
        MapElements.of(dataset.build())
            .using(e -> (X) dehydrated.call(e))
            .output());
  }

  public Stream<T> filter(Closure<?> predicate) {
    Closure<?> dehydrated = predicate.dehydrate();
    return descendant(() ->
        Filter.of(dataset.build())
            .by(e -> (Boolean) dehydrated.call(e))
            .output());
  }

  public Stream<Pair<Object, T>> withWindow() {
    return descendant(() ->
        FlatMap.of(dataset.build())
            .using((T in, Context<Pair<Object, T>> ctx) -> {
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

          }

          @Override
          public void close() throws IOException {

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
      LOG.error("Error in executing the flow", ex);
      throw new RuntimeException(ex);
    } finally {
      terminatingOperationCall.run();
    }
  }

  @SuppressWarnings("unchecked")
  public void persist(
      String host, int port,
      EntityDescriptor entity, AttributeDescriptor<T> attr) {

    // FIXME: this is wrong
    // need:
    //  - support Context.getTimestamp() in euphoria
    //  - support serialization of data in ingest API
    Dataset<Object> output = FlatMap.of((Dataset<Pair<String, byte[]>>) dataset.build())
        .using((in, ctx) -> {
          ctx.collect(Triple.of(in.getFirst(), in.getSecond(), System.currentTimeMillis()));
        })
        .output();

    output.persist((DataSink) new AttributeSink(host, port, entity, attr));

    runFlow(output.getFlow());
  }


  public WindowedStream<T> timeWindow(long millis) {
    return new TimeWindowedStream<>(
        executor, dataset, millis, terminatingOperationCall);
  }

  public WindowedStream<T> timeSlidingWindow(long millis, long slide) {
    return new TimeWindowedStream<>(
        executor, dataset, millis, slide, terminatingOperationCall);
  }

  @SuppressWarnings("unchecked")
  public WindowedStream<T> windowAll() {
    return new WindowedStream<>(
        executor, dataset, (Windowing) GlobalWindowing.get(), terminatingOperationCall);
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
