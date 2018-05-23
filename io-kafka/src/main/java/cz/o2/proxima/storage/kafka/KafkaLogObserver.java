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

import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.LogObserverBase;
import cz.o2.proxima.view.PartitionedLogObserver;
import java.util.Collection;

/**
 * A {@link PartitionedLogObserver} usable as {@link LogObserver}.
 */
interface KafkaLogObserver extends LogObserverBase {

  interface ConfirmCallback {
    void apply(boolean succ, Throwable err);
  }

  class LogObserverKafkaLogObserver implements KafkaLogObserver {

    final LogObserver observer;

    LogObserverKafkaLogObserver(LogObserver observer) {
      this.observer = observer;
    }

    @Override
    public void onRepartition(Collection<Partition> assigned) {
      // nop
    }

    @Override
    public boolean onNext(
        StreamElement ingest,
        ConfirmCallback confirm,
        Partition partition) {

      return observer.onNext(ingest, confirm::apply);
    }

    @Override
    public void onCompleted() {
      observer.onCompleted();
    }

    @Override
    public boolean onError(Throwable error) {
      return observer.onError(error);
    }

    @Override
    public void onCancelled() {
      observer.onCancelled();
    }

  }

  class PartitionedLogObserverKafkaLogObserver<T> implements KafkaLogObserver {

    public static <T> PartitionedLogObserverKafkaLogObserver<T> of(
        PartitionedLogObserver<T> wrap,
        Consumer<T> consumer) {

      return new PartitionedLogObserverKafkaLogObserver<>(wrap, consumer);
    }

    private final PartitionedLogObserver<T> observer;
    private final Consumer<T> outputConsumer;

    PartitionedLogObserverKafkaLogObserver(
        PartitionedLogObserver<T> wrap,
        Consumer<T> outputConsumer) {

      this.observer = wrap;
      this.outputConsumer = outputConsumer;
    }

    @Override
    public void onRepartition(Collection<Partition> assigned) {
      observer.onRepartition(assigned);
    }

    @Override
    public boolean onNext(
        StreamElement ingest, ConfirmCallback confirm,
        Partition partition) {

      return observer.onNext(ingest, confirm::apply, partition, outputConsumer);
    }

    @Override
    public void onCompleted() {
      observer.onCompleted();
    }

    @Override
    public boolean onError(Throwable error) {
      return observer.onError(error);
    }

    @Override
    public void onCancelled() {
      observer.onCancelled();
    }

  }

  void onRepartition(Collection<Partition> assigned);

  boolean onNext(StreamElement ingest, ConfirmCallback confirm, Partition partition);

}
