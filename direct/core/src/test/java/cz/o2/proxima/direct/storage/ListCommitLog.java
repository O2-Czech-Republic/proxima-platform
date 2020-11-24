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
package cz.o2.proxima.direct.storage;

import static cz.o2.proxima.direct.commitlog.ObserverUtils.asRepartitionContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.ObserverUtils;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryPredicate;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.WatermarkEstimator;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * A bounded {@link CommitLogReader} containing predefined data.
 *
 * <p>This is very simplistic implementation which just pushes all data to the provided observer.
 */
public class ListCommitLog implements CommitLogReader {

  private static final Partition PARTITION = () -> 0;

  private static final Map<String, List<StreamElement>> UUID_TO_DATA = new ConcurrentHashMap<>();
  private static final Map<String, Consumer> CONSUMERS = new ConcurrentHashMap<>();

  static class ListOffset implements Offset {

    private static final long serialVersionUID = 1L;

    @Getter private final String consumerName;
    @Getter final int offset;
    @Getter final long watermark;

    private ListOffset(String consumerName, int offset, long watermark) {
      this.consumerName = Objects.requireNonNull(consumerName);
      this.offset = offset;
      this.watermark = watermark;
    }

    @Override
    public Partition getPartition() {
      return PARTITION;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("offset", offset)
          .add("watermark", watermark)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ListOffset) {
        ListOffset other = (ListOffset) obj;
        return other.offset == this.offset && other.watermark == this.watermark;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return (int) ((offset ^ watermark) % Integer.MAX_VALUE);
    }
  }

  /**
   * Create the new {@link ListCommitLog}, with externalizable offsets (that are offsets that can be
   * persisted in external system - e.g. a checkpoint - and be sure they represent the same element
   * even upon recovery). Commit-logs with "externalizable offsets" (e.g. Apache Kafka) need not
   * rely on ack() and nack() of elements, as offsets can be taken and recovered independently of
   * the actual acknowledgements. Consumers are free to ack messages nevertheless.
   *
   * @param data the data to be present in the commit log
   * @param context {@link Context} for direct consumption
   * @return the commit-log
   */
  public static ListCommitLog of(List<StreamElement> data, Context context) {
    return of(data, null, context);
  }

  /**
   * Create the new {@link ListCommitLog}, with externalizable offsets (that are offsets that can be
   * persisted in external system - e.g. a checkpoint - and be sure they represent the same element
   * even upon recovery). Commit-logs with "externalizable offsets" (e.g. Apache Kafka) need not
   * rely on ack() and nack() of elements, as offsets can be taken and recovered independently of
   * the actual acknowledgements. Consumers are free to ack messages nevertheless.
   *
   * @param data the data to be present in the commit log
   * @param watermarkEstimator {@link WatermarkEstimator} that will be used to generate watermarks
   * @param context {@link Context} for direct consumption
   * @return the commit-log
   */
  public static ListCommitLog of(
      List<StreamElement> data, @Nullable WatermarkEstimator watermarkEstimator, Context context) {

    return new ListCommitLog(data, watermarkEstimator, context);
  }


  /**
   * Create the new {@link ListCommitLog}, which mimics non-externalizable offsets (that are offsets
   * that cannot be persisted in external system - e.g. a checkpoint - and be sure they represent
   * the same element even upon recovery). Commit-logs with "non-externalizable offsets" (e.g.
   * Google PubSub) rely heavily on ack() and nack() of elements to ensure at-least-once semantics
   * (typically not exactly-once-semantics, because when consumer consumes element and does neither
   * ack() nor nack() it until timeout, the element is resend to another (or the same) consumer).
   *
   *
   * @param data the data to be present in the commit log
   * @param context {@link Context} for direct consumption
   * @return the commit-log
   */
  public static ListCommitLog ofNonExternalizable(List<StreamElement> data, Context context) {
    return ofNonExternalizable(data, null, context);
  }

  /**
   * Create the new {@link ListCommitLog}, which mimics non-externalizable offsets (that are offsets
   * that cannot be persisted in external system - e.g. a checkpoint - and be sure they represent
   * the same element even upon recovery). Commit-logs with "non-externalizable offsets" (e.g.
   * Google PubSub) rely heavily on ack() and nack() of elements to ensure at-least-once semantics
   * (typically not exactly-once-semantics, because when consumer consumes element and does neither
   * ack() nor nack() it until timeout, the element is resend to another (or the same) consumer).
   *
   * @param data the data to be present in the commit log
   * @param watermarkEstimator {@link WatermarkEstimator} that will be used to generate watermarks
   * @param context {@link Context} for direct consumption
   * @return the commit-log
   */
  public static ListCommitLog ofNonExternalizable(
      List<StreamElement> data, @Nullable WatermarkEstimator watermarkEstimator, Context context) {

    return new ListCommitLog(data, false, watermarkEstimator, context);
  }

  @VisibleForTesting
  static final class ListObserveHandle implements ObserveHandle {

    @Getter private final String consumerName;

    @Getter private volatile boolean closed = false;

    ListObserveHandle(String consumerName) {
      this.consumerName = Objects.requireNonNull(consumerName);
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public List<Offset> getCommittedOffsets() {
      Consumer consumer = Objects.requireNonNull(CONSUMERS.get(consumerName));
      return consumer.getCommittedOffsets();
    }

    @Override
    public void resetOffsets(List<Offset> offsets) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Offset> getCurrentOffsets() {
      Consumer consumer = Objects.requireNonNull(CONSUMERS.get(consumerName));
      return consumer.getCurrentOffsets();
    }

    @Override
    public void waitUntilReady() {}

    @VisibleForTesting
    Consumer getConsumer() {
      return CONSUMERS.get(consumerName);
    }
  }

  @VisibleForTesting
  class Consumer {

    /**
     * UUID of the {@link cz.o2.proxima.direct.storage.ListCommitLog} that this consumer reads from.
     */
    @Getter private final String logUuid;

    private final String consumerName;

    @Getter
    private final Set<Integer> inflightOffsets = Collections.synchronizedSet(new HashSet<>());

    @Getter private final Set<Integer> ackedOffsets = Collections.synchronizedSet(new HashSet<>());

    private final Map<Integer, OffsetCommitter> offsetToContext = new ConcurrentHashMap<>();

    @Nullable private final WatermarkEstimator watermarkEstimator;

    /** Last offset pushed to consumer. */
    private int currentOffset = 0;

    private Consumer(
        String logUuid, String consumerName, @Nullable WatermarkEstimator watermarkEstimator) {
      this.logUuid = logUuid;
      this.consumerName = consumerName;
      this.watermarkEstimator = watermarkEstimator;
    }

    public long getWatermark() {
      return watermarkEstimator == null ? getWatermarkDefault() : watermarkEstimator.getWatermark();
    }

    private long getWatermarkDefault() {
      List<StreamElement> data = UUID_TO_DATA.get(logUuid);
      long watermark = Long.MAX_VALUE;
      for (int i = externalizableOffsets ? currentOffset : 0; i < data.size(); i++) {
        if (data.get(i).getStamp() < watermark && (externalizableOffsets || !isAcked(i))) {
          watermark = data.get(i).getStamp();
        }
      }
      return watermark;
    }

    List<Offset> getCommittedOffsets() {
      if (externalizableOffsets) {
        return Collections.emptyList();
      }
      return Collections.singletonList(new ListOffset(consumerName, -1, getWatermark()));
    }

    List<Offset> getCurrentOffsets() {
      return Collections.singletonList(
          externalizableOffsets
              ? new ListOffset(consumerName, currentOffset, getWatermark())
              : new ListOffset(consumerName, -1, getWatermark()));
    }

    public void moveCurrentOffset(int offset) {
      inflightOffsets.add(offset);
      currentOffset = Math.max(offset, currentOffset);
    }

    public void ack(int offset) {
      nack(offset);
      ackedOffsets.add(offset);
      if (watermarkEstimator != null) {
        watermarkEstimator.update(data().get(offset));
      }
    }

    public void nack(int offset) {
      inflightOffsets.remove(offset);
    }

    OnNextContext asOnNextContext(LogObserver.OffsetCommitter committer, int offset, boolean bulk) {
      ListOffset listOffset = new ListOffset(consumerName, offset, getWatermark());
      moveCurrentOffset(offset);
      final LogObserver.OffsetCommitter contextCommitter;
      LogObserver.OffsetCommitter singleCommitter =
          (succ, exc) -> {
            committer.commit(succ, exc);
            if (succ) {
              ack(offset);
            } else {
              nack(offset);
            }
          };
      if (bulk) {
        contextCommitter =
            (succ, exc) -> {
              synchronized (inflightOffsets) {
                // clone to prevent ConcurrentModificationException
                new ArrayList<>(inflightOffsets)
                    .stream()
                    .filter(o -> o <= offset && !ackedOffsets.contains(o))
                    .map(offsetToContext::remove)
                    .filter(Objects::nonNull)
                    .forEach(p -> p.commit(succ, exc));
              }
            };
      } else {
        contextCommitter = singleCommitter;
      }
      OnNextContext context = ObserverUtils.asOnNextContext(contextCommitter, listOffset);
      offsetToContext.put(offset, singleCommitter);
      return context;
    }

    boolean isAcked(int offset) {
      return ackedOffsets.contains(offset);
    }
  }

  private final String uuid;
  private final boolean externalizableOffsets;
  @Nullable private final WatermarkEstimator watermarkEstimator;
  private final Context context;
  private transient ExecutorService executor;

  private ListCommitLog(
      List<StreamElement> data, @Nullable WatermarkEstimator watermarkEstimator, Context context) {

    this(data, true, watermarkEstimator, context);
  }

  private ListCommitLog(
      List<StreamElement> data,
      boolean externalizableOffsets,
      @Nullable WatermarkEstimator watermarkEstimator,
      Context context) {

    this.uuid = UUID.randomUUID().toString();
    UUID_TO_DATA.put(uuid, Collections.unmodifiableList(new ArrayList<>(data)));
    this.externalizableOffsets = externalizableOffsets;
    this.watermarkEstimator = watermarkEstimator;
    this.context = context;
  }

  private ListCommitLog(
      String uuid,
      boolean externalizableOffsets,
      @Nullable WatermarkEstimator watermarkEstimator,
      Context context) {

    this.uuid = uuid;
    this.externalizableOffsets = externalizableOffsets;
    this.watermarkEstimator = watermarkEstimator;
    this.context = context;
  }

  @Override
  public URI getUri() {
    try {
      return new URI("list://" + this);
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public List<Partition> getPartitions() {
    return Collections.singletonList(PARTITION);
  }

  @Override
  public ObserveHandle observe(@Nullable String name, Position position, LogObserver observer) {
    String consumerName = name == null ? UUID.randomUUID().toString() : name;
    Consumer consumer =
        CONSUMERS.computeIfAbsent(
            consumerName, k -> new Consumer(uuid, consumerName, watermarkEstimator));
    ListObserveHandle handle = new ListObserveHandle(consumerName);
    pushTo(
        (element, offset) -> {
          if (handle.isClosed()) {
            return false;
          }
          return observer.onNext(
              element,
              consumer.asOnNextContext(
                  (succ, exc) -> {
                    if (exc != null) {
                      observer.onError(exc);
                    }
                  },
                  offset,
                  false));
        },
        observer::onCompleted,
        observer::onCancelled);
    return handle;
  }

  private List<StreamElement> data() {
    return Objects.requireNonNull(UUID_TO_DATA.get(uuid));
  }

  @Override
  public ObserveHandle observePartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    return observe(name, position, observer);
  }

  @Override
  public ObserveHandle observeBulk(
      @Nullable String name, Position position, boolean stopAtCurrent, LogObserver observer) {

    String consumerName = name == null ? UUID.randomUUID().toString() : name;
    return pushToObserver(consumerName, 0, true, observer);
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    return observeBulk(name, position, observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(Collection<Offset> offsets, LogObserver observer) {
    Set<String> consumers =
        offsets.stream().map(o -> ((ListOffset) o).getConsumerName()).collect(Collectors.toSet());
    final String name = Iterables.getOnlyElement(consumers);
    if (externalizableOffsets) {
      ListOffset offset = (ListOffset) Iterables.getOnlyElement(offsets);
      return pushToObserver(name, offset.getOffset() + 1, true, observer);
    }
    final Consumer consumer = Objects.requireNonNull(CONSUMERS.get(name));
    return pushToObserver(
        name,
        o -> !consumer.getAckedOffsets().contains(o) && !consumer.getInflightOffsets().contains(o),
        true,
        observer);
  }

  @Override
  public Factory<?> asFactory() {
    final String uuid = this.uuid;
    final Context context = this.context;
    final boolean externalizableOffsets = this.externalizableOffsets;
    final WatermarkEstimator watermarkEstimator = this.watermarkEstimator;
    return repo -> new ListCommitLog(uuid, externalizableOffsets, watermarkEstimator, context);
  }

  @Override
  public boolean hasExternalizableOffsets() {
    return externalizableOffsets;
  }

  private ObserveHandle pushToObserver(
      @Nonnull String name, int skip, boolean bulk, LogObserver observer) {

    AtomicInteger skipCounter = new AtomicInteger(skip);
    return pushToObserver(name, offset -> skipCounter.decrementAndGet() <= 0, bulk, observer);
  }

  private ObserveHandle pushToObserver(
      @Nonnull String name,
      UnaryPredicate<Integer> allowOffsetPredicate,
      boolean bulk,
      LogObserver observer) {

    observer.onRepartition(asRepartitionContext(Collections.singletonList(PARTITION)));
    Consumer consumer =
        CONSUMERS.computeIfAbsent(name, k -> new Consumer(uuid, name, watermarkEstimator));
    ListObserveHandle handle = new ListObserveHandle(name);
    pushTo(
        (element, offset) -> {
          if (handle.isClosed()) {
            return false;
          }
          if (allowOffsetPredicate.apply(offset)) {
            return observer.onNext(
                element,
                consumer.asOnNextContext(
                    (succ, exc) -> {
                      if (!succ) {
                        observer.onError(exc);
                      }
                    },
                    offset,
                    bulk));
          }
          return true;
        },
        externalizableOffsets ? () -> true : allMatchOffset(consumer::isAcked),
        observer::onCompleted,
        observer::onCancelled);
    return handle;
  }

  private cz.o2.proxima.functional.Factory<Boolean> allMatchOffset(
      UnaryPredicate<Integer> offsetPredicate) {
    return () -> IntStream.range(0, data().size()).allMatch(offsetPredicate::apply);
  }

  private void pushTo(
      BiFunction<StreamElement, Integer, Boolean> consumer,
      Runnable onFinished,
      Runnable onCancelled) {

    pushTo(consumer, () -> true, onFinished, onCancelled);
  }

  private void pushTo(
      BiFunction<StreamElement, Integer, Boolean> consumer,
      cz.o2.proxima.functional.Factory<Boolean> finishedCheck,
      Runnable onFinished,
      Runnable onCancelled) {

    executor()
        .execute(
            () -> {
              do {
                int index = 0;
                for (StreamElement el : data()) {
                  if (!consumer.apply(el, index++)) {
                    onCancelled.run();
                    return;
                  }
                }
              } while (!finishedCheck.apply());
              onFinished.run();
            });
  }

  private ExecutorService executor() {
    if (executor == null) {
      executor = context.getExecutorService();
    }
    return executor;
  }
}
