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
package cz.o2.proxima.beam.tools.groovy;

import cz.o2.proxima.beam.core.PCollectionTools;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.tools.groovy.WindowedStream;
import cz.o2.proxima.util.Pair;
import groovy.lang.Closure;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link WindowedStream} backed by beam.
 */
class BeamWindowedStream<T> extends BeamStream<T> implements WindowedStream<T> {

  BeamWindowedStream(boolean bounded, PCollectionProvider<T> input) {
    super(bounded, input);
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor, Closure<V> valueExtractor,
      V initialValue, Closure<V> reducer) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> reduce(
      Closure<K> keyExtractor, V initialValue, Closure<V> reducer) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowedStream<StreamElement> reduceToLatest() {
    return new BeamWindowedStream<>(
        bounded,
        pipeline -> PCollectionTools.reduceAsSnapshot(
            (PCollection) collection.materialize(pipeline)));
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> flatReduce(
      Closure<K> keyExtractor, Closure<V> listReduce) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <K, V> WindowedStream<Pair<K, V>> combine(
      Closure<K> keyExtractor, Closure<V> valueExtractor,
      V initial, Closure<V> combine) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <K> WindowedStream<Pair<K, T>> combine(
      Closure<K> keyExtractor, T initial, Closure<T> combine) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <K> WindowedStream<Pair<K, Long>> countByKey(
      Closure<K> keyExtractor) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public WindowedStream<Double> average(Closure<Double> valueExtractor) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <K> WindowedStream<Pair<K, Double>> averageByKey(
      Closure<K> keyExtractor, Closure<Double> valueExtractor) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <LEFT, RIGHT> WindowedStream<Pair<T, RIGHT>> join(
      WindowedStream<RIGHT> right, Closure<LEFT> leftKey,
      Closure<RIGHT> rightKey) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <LEFT, RIGHT> WindowedStream<Pair<T, RIGHT>> leftJoin(
      WindowedStream<RIGHT> right, Closure<LEFT> leftKey,
      Closure<RIGHT> rightKey) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public WindowedStream<T> sorted(Closure<Integer> compareFn) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public WindowedStream<Comparable<T>> sorted() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public WindowedStream<Long> count() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public WindowedStream<Double> sum(Closure<Double> valueExtractor) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <K> WindowedStream<Pair<K, Double>> sumByKey(
      Closure<K> keyExtractor, Closure<Double> valueExtractor) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public WindowedStream<T> distinct() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public WindowedStream<T> distinct(Closure<?> mapper) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public WindowedStream<T> withEarlyEmitting(long duration) {
    throw new UnsupportedOperationException("Not supported yet.");
  }


}
