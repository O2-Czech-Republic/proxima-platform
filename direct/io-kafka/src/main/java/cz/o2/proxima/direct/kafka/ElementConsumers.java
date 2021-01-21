/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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

import static cz.o2.proxima.direct.commitlog.ObserverUtils.asOnIdleContext;
import static cz.o2.proxima.direct.commitlog.ObserverUtils.asOnNextContext;
import static cz.o2.proxima.direct.commitlog.ObserverUtils.asRepartitionContext;

import com.google.common.base.MoreObjects;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.functional.BiConsumer;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.WatermarkSupplier;
import cz.o2.proxima.time.Watermarks;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/** Placeholder class for {@link ElementConsumer ElementConsumers}. */
@Slf4j
class ElementConsumers {

  private abstract static class ConsumerBase<K, V> implements ElementConsumer<K, V> {

    final Map<Integer, Long> committed = Collections.synchronizedMap(new HashMap<>());
    final Map<Integer, Long> processing = Collections.synchronizedMap(new HashMap<>());
    long watermark;

    @Override
    public void onCompleted() {
      observer().onCompleted();
    }

    @Override
    public void onCancelled() {
      observer().onCancelled();
    }

    @Override
    public boolean onError(Throwable err) {
      return observer().onError(err);
    }

    @Override
    public void onAssign(KafkaConsumer<K, V> consumer, Collection<TopicOffset> offsets) {
      committed.clear();
      committed.putAll(
          offsets
              .stream()
              .collect(Collectors.toMap(o -> o.getPartition().getId(), TopicOffset::getOffset)));
      processing.clear();
      offsets.forEach(tp -> processing.put(tp.getPartition().getId(), tp.getOffset() - 1));
    }

    @Override
    public List<TopicOffset> getCurrentOffsets() {
      return TopicOffset.fromMap(processing, watermark);
    }

    @Override
    public List<TopicOffset> getCommittedOffsets() {
      return TopicOffset.fromMap(committed, watermark);
    }

    abstract LogObserver observer();
  }

  static final class OnlineConsumer<K, V> extends ConsumerBase<K, V> {

    private final LogObserver observer;
    private final OffsetCommitter<TopicPartition> committer;
    private final Factory<Map<TopicPartition, OffsetAndMetadata>> prepareCommit;

    OnlineConsumer(
        LogObserver observer,
        OffsetCommitter<TopicPartition> committer,
        Factory<Map<TopicPartition, OffsetAndMetadata>> prepareCommit) {

      this.observer = observer;
      this.committer = committer;
      this.prepareCommit = prepareCommit;
    }

    @Override
    public boolean consumeWithConfirm(
        @Nullable StreamElement element,
        TopicPartition tp,
        long offset,
        WatermarkSupplier watermarkSupplier,
        Consumer<Throwable> errorHandler) {

      processing.put(tp.partition(), offset);
      watermark = watermarkSupplier.getWatermark();
      if (element != null && watermark < Watermarks.MAX_WATERMARK) {
        return observer.onNext(
            element,
            asOnNextContext(
                (succ, exc) -> {
                  if (succ) {
                    committed.compute(
                        tp.partition(), (k, v) -> v == null || v <= offset ? offset + 1 : v);
                    committer.confirm(tp, offset);
                  } else if (exc != null) {
                    errorHandler.accept(exc);
                  }
                },
                new TopicOffset(tp.partition(), offset, watermark)));
      }
      committed.compute(tp.partition(), (k, v) -> v == null || v <= offset ? offset + 1 : v);
      committer.confirm(tp, offset);
      return watermark < Watermarks.MAX_WATERMARK;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> prepareOffsetsForCommit() {
      return prepareCommit.apply();
    }

    @Override
    LogObserver observer() {
      return observer;
    }

    @Override
    public void onAssign(KafkaConsumer<K, V> consumer, Collection<TopicOffset> offsets) {
      super.onAssign(consumer, offsets);
      committer.clear();
      observer.onRepartition(
          asRepartitionContext(
              offsets.stream().map(TopicOffset::getPartition).collect(Collectors.toList())));
    }

    @Override
    public void onStart() {
      committer.clear();
    }

    @Override
    public void onIdle(WatermarkSupplier watermarkSupplier) {
      observer.onIdle(asOnIdleContext(watermarkSupplier));
    }
  }

  static final class BulkConsumer<K, V> extends ConsumerBase<K, V> {

    private final String topic;
    private final LogObserver observer;
    private final BiConsumer<TopicPartition, Long> commit;
    private final Factory<Map<TopicPartition, OffsetAndMetadata>> prepareCommit;
    private final Runnable onStart;

    BulkConsumer(
        String topic,
        LogObserver observer,
        BiConsumer<TopicPartition, Long> commit,
        Factory<Map<TopicPartition, OffsetAndMetadata>> prepareCommit,
        Runnable onStart) {

      this.topic = topic;
      this.observer = observer;
      this.commit = commit;
      this.prepareCommit = prepareCommit;
      this.onStart = onStart;
    }

    @Override
    public boolean consumeWithConfirm(
        @Nullable StreamElement element,
        TopicPartition tp,
        long offset,
        WatermarkSupplier watermarkSupplier,
        Consumer<Throwable> errorHandler) {
      processing.put(tp.partition(), offset);
      watermark = watermarkSupplier.getWatermark();
      if (element != null) {
        return observer.onNext(element, context(tp, offset, watermarkSupplier, errorHandler));
      }
      return true;
    }

    private OnNextContext context(
        TopicPartition tp,
        long offset,
        WatermarkSupplier watermarkSupplier,
        Consumer<Throwable> errorHandler) {

      Map<Integer, Long> toCommit = new HashMap<>(this.processing);
      return asOnNextContext(
          (succ, err) -> {
            if (succ) {
              toCommit.forEach(
                  (part, off) ->
                      committed.compute(
                          part, (k, v) -> Math.max(MoreObjects.firstNonNull(v, 0L), off + 1)));
              committed.forEach((p, o) -> commit.accept(new TopicPartition(tp.topic(), p), o));
            } else if (err != null) {
              errorHandler.accept(err);
            }
          },
          new TopicOffset(tp.partition(), offset, watermarkSupplier.getWatermark()));
    }

    @Override
    LogObserver observer() {
      return observer;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> prepareOffsetsForCommit() {
      return prepareCommit.apply();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onAssign(KafkaConsumer<K, V> consumer, Collection<TopicOffset> offsets) {
      super.onAssign(consumer, offsets);
      observer.onRepartition(
          asRepartitionContext(
              offsets.stream().map(TopicOffset::getPartition).collect(Collectors.toList())));

      Utils.seekToOffsets(topic, (List) offsets, consumer);
    }

    @Override
    public void onStart() {
      onStart.run();
    }

    @Override
    public void onIdle(WatermarkSupplier watermarkSupplier) {
      observer.onIdle(asOnIdleContext(watermarkSupplier));
    }
  }

  private ElementConsumers() {
    // nop
  }
}
