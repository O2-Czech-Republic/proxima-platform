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
package cz.o2.proxima.beam.core;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.beam.core.io.AttributeDescriptorCoder;
import cz.o2.proxima.beam.core.io.EntityDescriptorCoder;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.AttributeFamilyProxyDescriptor;
import cz.o2.proxima.repository.DataOperator;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.internal.DataAccessorLoader;
import cz.o2.proxima.util.Pair;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow.IntervalWindowCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link DataOperator} for Apache Beam transformations. */
public class BeamDataOperator implements DataOperator {

  @FunctionalInterface
  private interface PCollectionFactoryFromDescriptor<T extends PCollectionDescriptor> {
    PCollection<StreamElement> apply(T desc);
  }

  // labelling interface
  private interface PCollectionDescriptor {}

  @Value
  private class StreamDescriptor implements PCollectionDescriptor {
    Pipeline pipeline;
    DataAccessor dataAccessor;
    @Nullable String name;
    Position position;
    boolean stopAtCurrent;
    boolean useEventTime;

    PCollection<StreamElement> createStream(long limit) {
      return dataAccessor
          .createStream(name, pipeline, position, stopAtCurrent, useEventTime, limit)
          .setTypeDescriptor(TypeDescriptor.of(StreamElement.class));
    }
  }

  @Value
  private class BatchUpdatesDescriptor implements PCollectionDescriptor {
    Pipeline pipeline;
    DataAccessor dataAccessor;
    long startStamp;
    long endStamp;
    boolean asStream;

    PCollection<StreamElement> createBatchUpdates(List<AttributeDescriptor<?>> attrList) {
      return asStream
          ? dataAccessor.createStreamFromUpdates(pipeline, attrList, startStamp, endStamp, -1)
          : dataAccessor.createBatch(pipeline, attrList, startStamp, endStamp);
    }
  }

  @Value
  private final class BatchSnapshotDescriptor implements PCollectionDescriptor {
    private final Pipeline pipeline;
    private final DataAccessor dataAccessor;
    private final long fromStamp;
    private final long untilStamp;

    PCollection<StreamElement> createBatchUpdates(List<AttributeDescriptor<?>> attrList) {
      return dataAccessor.createBatch(pipeline, attrList, fromStamp, untilStamp);
    }
  }

  private final Repository repo;
  private final @Nullable DirectDataOperator direct;
  private final DataAccessorLoader<BeamDataOperator, DataAccessor, DataAccessorFactory> loader;
  private final Map<AttributeFamilyDescriptor, DataAccessor> accessorMap;
  private final Map<PCollectionDescriptor, PCollection<StreamElement>> createdStreamsMap =
      Collections.synchronizedMap(new HashMap<>());
  private final Set<Pipeline> typesRegistered = new HashSet<>();

  BeamDataOperator(Repository repo) {
    this.repo = repo;
    this.accessorMap = Collections.synchronizedMap(new HashMap<>());
    this.loader = DataAccessorLoader.of(repo, DataAccessorFactory.class);
    this.direct =
        repo.hasOperator("direct") ? repo.getOrCreateOperator(DirectDataOperator.class) : null;
  }

  @Override
  public void close() {
    if (direct != null) {
      direct.close();
    }
    reload();
  }

  @Override
  public void reload() {
    accessorMap.clear();
    createdStreamsMap.clear();
    typesRegistered.clear();
  }

  /**
   * Create {@link PCollection} in given {@link Pipeline} from commit log for given attributes.
   *
   * @param pipeline the {@link Pipeline} to create {@link PCollection} in.
   * @param position position in commit log to read from
   * @param stopAtCurrent {@code true} to stop at recent data
   * @param useEventTime {@code true} to use event time
   * @param attrs the attributes to create {@link PCollection} for
   * @return the {@link PCollection}
   */
  @SafeVarargs
  public final PCollection<StreamElement> getStream(
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean useEventTime,
      AttributeDescriptor<?>... attrs) {

    return getStream(null, pipeline, position, stopAtCurrent, useEventTime, attrs);
  }

  /**
   * Create {@link PCollection} in given {@link Pipeline} from commit log for given attributes.
   *
   * @param name name of the consumer
   * @param pipeline the {@link Pipeline} to create {@link PCollection} in.
   * @param position position in commit log to read from
   * @param stopAtCurrent {@code true} to stop at recent data
   * @param useEventTime {@code true} to use event time
   * @param attrs the attributes to create {@link PCollection} for
   * @return the {@link PCollection}
   */
  @SafeVarargs
  public final PCollection<StreamElement> getStream(
      @Nullable String name,
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean useEventTime,
      AttributeDescriptor<?>... attrs) {

    return getStream(name, pipeline, position, stopAtCurrent, useEventTime, Long.MAX_VALUE, attrs);
  }

  /**
   * Create {@link PCollection} in given {@link Pipeline} from commit log for given attributes
   * limiting number of elements read.
   *
   * @param name name of the consumer
   * @param pipeline the {@link Pipeline} to create {@link PCollection} in.
   * @param position position in commit log to read from
   * @param stopAtCurrent {@code true} to stop at recent data
   * @param useEventTime {@code true} to use event time
   * @param limit number of elements to read from the source
   * @param attrs the attributes to create {@link PCollection} for
   * @return the {@link PCollection}
   */
  @VisibleForTesting
  @SafeVarargs
  final PCollection<StreamElement> getStream(
      @Nullable String name,
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean useEventTime,
      long limit,
      AttributeDescriptor<?>... attrs) {

    return findSuitableAccessors(af -> af.getAccess().canReadCommitLog(), "commit-log", attrs)
        .map(
            da -> {
              StreamDescriptor desc =
                  new StreamDescriptor(pipeline, da, name, position, stopAtCurrent, useEventTime);
              return getOrCreatePCollection(
                  desc, limit < 0 || limit == Long.MAX_VALUE, d -> d.createStream(limit));
            })
        .reduce((left, right) -> Union.of(left, right).output())
        .orElseThrow(failNotFound(attrs, "commit-log"))
        .apply(filterAttrs(attrs));
  }

  /**
   * Create {@link PCollection} from updates to given attributes.
   *
   * @param pipeline {@link Pipeline} to create the {@link PCollection} in
   * @param attrs attributes to read updates for
   * @return the {@link PCollection}
   */
  @SafeVarargs
  public final PCollection<StreamElement> getBatchUpdates(
      Pipeline pipeline, AttributeDescriptor<?>... attrs) {

    return getBatchUpdates(pipeline, Long.MIN_VALUE, Long.MAX_VALUE, attrs);
  }

  /**
   * Create {@link PCollection} from updates to given attributes with given time range.
   *
   * @param pipeline {@link Pipeline} to create the {@link PCollection} in
   * @param startStamp timestamp (inclusive) of first update taken into account
   * @param endStamp timestamp (exclusive) of last update taken into account
   * @param attrs attributes to read updates for
   * @return the {@link PCollection}
   */
  @SafeVarargs
  public final PCollection<StreamElement> getBatchUpdates(
      Pipeline pipeline, long startStamp, long endStamp, AttributeDescriptor<?>... attrs) {

    return getBatchUpdates(pipeline, startStamp, endStamp, false, attrs);
  }

  /**
   * Create {@link PCollection} from updates to given attributes with given time range.
   *
   * @param pipeline {@link Pipeline} to create the {@link PCollection} in
   * @param startStamp timestamp (inclusive) of first update taken into account
   * @param endStamp timestamp (exclusive) of last update taken into account
   * @param asStream create PCollection that is suitable for streaming processing (i.e. can update
   *     watermarks before end of input)
   * @param attrs attributes to read updates for
   * @return the {@link PCollection}
   */
  @SafeVarargs
  public final PCollection<StreamElement> getBatchUpdates(
      Pipeline pipeline,
      long startStamp,
      long endStamp,
      boolean asStream,
      AttributeDescriptor<?>... attrs) {

    List<AttributeDescriptor<?>> attrClosure =
        findSuitableFamilies(af -> af.getAccess().canReadBatchUpdates(), attrs)
            .filter(p -> p.getSecond().isPresent())
            .map(p -> p.getSecond().get())
            .flatMap(d -> d.getAttributes().stream())
            .distinct()
            .collect(Collectors.toList());
    AttributeDescriptor<?>[] closureAsArray = attrClosure.toArray(new AttributeDescriptor[0]);

    return findSuitableAccessors(
            af -> af.getAccess().canReadBatchUpdates(), "batch-updates", closureAsArray)
        .map(
            da -> {
              BatchUpdatesDescriptor desc =
                  new BatchUpdatesDescriptor(pipeline, da, startStamp, endStamp, asStream);
              return getOrCreatePCollection(desc, true, d -> d.createBatchUpdates(attrClosure));
            })
        .reduce((left, right) -> Union.of(left, right).output())
        .orElseThrow(failNotFound(attrs, "batch-updates"))
        .apply(filterAttrs(attrs));
  }

  /**
   * Create {@link PCollection} from snapshot of given attributes. The snapshot is either read from
   * available storage or created by reduction of updates.
   *
   * @param pipeline {@link Pipeline} to create the {@link PCollection} in
   * @param attrs attributes to read snapshot for
   * @return the {@link PCollection}
   */
  public final PCollection<StreamElement> getBatchSnapshot(
      Pipeline pipeline, AttributeDescriptor<?>... attrs) {

    return getBatchSnapshot(pipeline, Long.MIN_VALUE, Long.MAX_VALUE, attrs);
  }

  /**
   * Create {@link PCollection} from snapshot of given attributes. The snapshot is either read from
   * available storage or created by reduction of updates.
   *
   * @param pipeline {@link Pipeline} to create the {@link PCollection} in
   * @param fromStamp ignore updates older than this stamp
   * @param untilStamp read only updates older than this timestamp (i.e. if this method was called
   *     at the given timestamp)
   * @param attrs attributes to read snapshot for
   * @return the {@link PCollection}
   */
  public final PCollection<StreamElement> getBatchSnapshot(
      Pipeline pipeline, long fromStamp, long untilStamp, AttributeDescriptor<?>... attrs) {

    List<AttributeDescriptor<?>> attrList = Arrays.stream(attrs).collect(Collectors.toList());

    List<Pair<AttributeDescriptor<?>, Optional<AttributeFamilyDescriptor>>> resolvedAttrs;
    resolvedAttrs =
        findSuitableFamilies(af -> af.getAccess().canReadBatchSnapshot(), attrs)
            .collect(Collectors.toList());

    boolean unresolved = resolvedAttrs.stream().anyMatch(p -> !p.getSecond().isPresent());

    if (!unresolved) {
      return resolvedAttrs
          .stream()
          // take all attributes from the same family
          // it will be filtered away then, this is needed to enable fusion of multiple reads from
          // the same family
          .flatMap(
              p ->
                  p.getSecond()
                      .get()
                      .getAttributes()
                      .stream()
                      .map(a -> Pair.of(a, p.getSecond().get())))
          .map(Pair::getSecond)
          .distinct()
          .map(this::accessorFor)
          .distinct()
          .map(
              da -> {
                BatchSnapshotDescriptor desc =
                    new BatchSnapshotDescriptor(pipeline, da, fromStamp, untilStamp);
                return getOrCreatePCollection(desc, true, d -> d.createBatchUpdates(attrList));
              })
          .reduce((left, right) -> Union.of(left, right).output())
          .orElseThrow(failNotFound(attrs, "batch-snapshot"))
          .apply(filterAttrs(attrs));
    }
    return PCollectionTools.reduceAsSnapshot(
        "getBatchSnapshot:" + Arrays.toString(attrs),
        getBatchUpdates(pipeline, fromStamp, untilStamp, attrs));
  }

  /**
   * Get {@link DataAccessor} for given {@link AttributeFamilyDescriptor}.
   *
   * <p>Needed for low-level access handling.
   *
   * @param family descriptor of family to retrieve accessor for
   * @return {@link DataAccessor} for given family
   */
  public DataAccessor getAccessorFor(AttributeFamilyDescriptor family) {
    return accessorFor(family);
  }

  @SuppressWarnings("unchecked")
  private Stream<DataAccessor> findSuitableAccessors(
      Predicate<AttributeFamilyDescriptor> predicate,
      String accessorType,
      AttributeDescriptor<?>[] attrs) {

    return findSuitableFamilies(predicate, attrs)
        .map(
            p -> {
              if (!p.getSecond().isPresent()) {
                throw new IllegalArgumentException(
                    "Missing " + accessorType + " for " + p.getFirst());
              }
              return p.getSecond().get();
            })
        .distinct()
        .map(this::accessorFor);
  }

  @SuppressWarnings("unchecked")
  private Stream<Pair<AttributeDescriptor<?>, Optional<AttributeFamilyDescriptor>>>
      findSuitableFamilies(
          Predicate<AttributeFamilyDescriptor> predicate, AttributeDescriptor<?>[] attrs) {

    return Arrays.stream(attrs)
        .map(
            desc ->
                Pair.of(
                    desc,
                    repo.getFamiliesForAttribute(desc)
                        .stream()
                        .filter(predicate)
                        // primary families have precedence
                        .min(Comparator.comparingInt(a -> a.getType().ordinal()))));
  }

  private DataAccessor accessorFor(AttributeFamilyDescriptor family) {
    synchronized (accessorMap) {
      if (family.isProxy()) {
        // prevent ConcurrentModificationException
        accessorFor(family.toProxy().getTargetFamilyRead());
        accessorFor(family.toProxy().getTargetFamilyWrite());
      }
      return accessorMap.computeIfAbsent(family, this::createAccessorFor);
    }
  }

  private DataAccessor createAccessorFor(AttributeFamilyDescriptor family) {

    if (family.isProxy()) {
      AttributeFamilyProxyDescriptor proxy = family.toProxy();
      DataAccessor readAccessor = accessorFor(proxy.getTargetFamilyRead());
      DataAccessor writeAccessor = accessorFor(proxy.getTargetFamilyWrite());
      return AttributeFamilyProxyDataAccessor.of(proxy, readAccessor, writeAccessor);
    }
    URI uri = family.getStorageUri();
    return loader
        .findForUri(uri)
        .map(f -> f.createAccessor(this, family.getEntity(), uri, family.getCfg()))
        .orElseThrow(
            () -> new IllegalStateException("No accessor for URI " + family.getStorageUri()));
  }

  @Override
  public Repository getRepository() {
    return repo;
  }

  public DirectDataOperator getDirect() {
    return Objects.requireNonNull(direct);
  }

  public boolean hasDirect() {
    return direct != null;
  }

  private PTransform<PCollection<StreamElement>, PCollection<StreamElement>> filterAttrs(
      AttributeDescriptor<?>[] attrs) {

    Set<AttributeDescriptor<?>> attrSet = Arrays.stream(attrs).collect(Collectors.toSet());
    return new PTransform<PCollection<StreamElement>, PCollection<StreamElement>>() {
      @Override
      public PCollection<StreamElement> expand(PCollection<StreamElement> input) {
        return input
            .apply(Filter.by(el -> attrSet.contains(el.getAttributeDescriptor())))
            .setTypeDescriptor(TypeDescriptor.of(StreamElement.class));
      }
    };
  }

  private Supplier<IllegalArgumentException> failNotFound(
      AttributeDescriptor<?>[] attrs, String accessorType) {
    Set<AttributeFamilyDescriptor> families =
        Arrays.stream(attrs)
            .flatMap(a -> repo.getFamiliesForAttribute(a).stream())
            .collect(Collectors.toSet());
    return () ->
        new IllegalArgumentException(
            String.format(
                "Failed to find suitable family type [%s] in [%s]", accessorType, families));
  }

  private <T extends PCollectionDescriptor> PCollection<StreamElement> getOrCreatePCollection(
      T desc, boolean cacheable, PCollectionFactoryFromDescriptor<T> factory) {

    final PCollection<StreamElement> ret;
    if (cacheable) {
      synchronized (createdStreamsMap) {
        PCollection<StreamElement> current = createdStreamsMap.get(desc);
        if (current == null) {
          ret = factory.apply(desc);
          createdStreamsMap.put(desc, ret);
        } else {
          ret = current;
        }
      }
    } else {
      // when limit is applied we must create a new source for each input
      ret = factory.apply(desc);
    }
    if (!typesRegisteredFor(ret.getPipeline())) {
      registerTypesFor(ret.getPipeline());
    }
    return ret;
  }

  private void registerTypesFor(Pipeline pipeline) {
    CoderRegistry registry = pipeline.getCoderRegistry();
    registry.registerCoderForClass(GlobalWindow.class, GlobalWindow.Coder.INSTANCE);
    registry.registerCoderForClass(IntervalWindow.class, IntervalWindowCoder.of());
    registry.registerCoderForClass(StreamElement.class, StreamElementCoder.of(repo));
    registry.registerCoderForClass(EntityDescriptor.class, EntityDescriptorCoder.of(repo));
    registry.registerCoderForClass(AttributeDescriptor.class, AttributeDescriptorCoder.of(repo));
    typesRegistered.add(pipeline);
  }

  private boolean typesRegisteredFor(Pipeline pipeline) {
    return typesRegistered.contains(pipeline);
  }
}
