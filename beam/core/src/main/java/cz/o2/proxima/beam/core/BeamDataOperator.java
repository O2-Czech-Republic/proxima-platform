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
package cz.o2.proxima.beam.core;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.AttributeFamilyProxyDescriptor;
import cz.o2.proxima.repository.DataOperator;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link DataOperator} for Apache Beam transformations.
 */
public class BeamDataOperator implements DataOperator {

  private final Repository repo;
  @Nullable
  private final DirectDataOperator direct;
  private final DataAccessorLoader<
        BeamDataOperator,
        DataAccessor,
        DataAccessorFactory> loader;
  private final Map<AttributeFamilyDescriptor, DataAccessor> accessorMap;

  BeamDataOperator(Repository repo) {
    this.repo = repo;
    this.accessorMap = Collections.synchronizedMap(new HashMap<>());
    this.loader = DataAccessorLoader.of(repo, DataAccessorFactory.class);
    this.direct = repo.hasOperator("direct")
        ? repo.getOrCreateOperator(DirectDataOperator.class)
        : null;
  }

  @Override
  public void close() {
    direct.close();
    accessorMap.clear();
  }

  @Override
  public void reload() {
    // nop
  }

  /**
   * Create {@link PCollection} in given {@link Pipeline} from commit log
   * for given attributes.
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

    return getStream(
        null, pipeline, position, stopAtCurrent, useEventTime, attrs);
  }

  /**
   * Create {@link PCollection} in given {@link Pipeline} from commit log
   * for given attributes.
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

    return getStream(
        name, pipeline, position, stopAtCurrent, useEventTime,
        Long.MAX_VALUE, attrs);
  }

  /**
   * Create {@link PCollection} in given {@link Pipeline} from commit log
   * for given attributes limiting number of elements read.
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
      String name,
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean useEventTime,
      long limit,
      AttributeDescriptor<?>... attrs) {

    return findSuitableAccessors(
        af -> af.getAccess().canReadCommitLog(), "commit-log", attrs)
        .map(da -> da.createStream(
            name, pipeline, position, stopAtCurrent, useEventTime, limit))
        .reduce((left, right) -> Union.of(left, right).output())
        .orElseThrow(failEmpty());
  }

  /**
   * Create {@link PCollection} from updates to given attributes.
   * @param pipeline {@link Pipeline} to create the {@link PCollection} in
   * @param attrs attributes to read updates for
   * @return the {@link PCollection}
   */
  @SafeVarargs
  public final PCollection<StreamElement> getBatchUpdates(
      Pipeline pipeline,
      AttributeDescriptor<?>... attrs) {

    return getBatchUpdates(pipeline, Long.MIN_VALUE, Long.MAX_VALUE, attrs);
  }

  /**
   * Create {@link PCollection} from updates to given attributes with given
   * time range.
   * @param pipeline {@link Pipeline} to create the {@link PCollection} in
   * @param startStamp timestamp (inclusive) of first update taken into account
   * @param endStamp timestamp (exclusive) of last update taken into account
   * @param attrs attributes to read updates for
   * @return the {@link PCollection}
   */
  @SafeVarargs
  public final PCollection<StreamElement> getBatchUpdates(
      Pipeline pipeline,
      long startStamp,
      long endStamp,
      AttributeDescriptor<?>... attrs) {

    List<AttributeDescriptor<?>> attrList = Arrays.stream(attrs)
        .collect(Collectors.toList());

    return findSuitableAccessors(
        af -> af.getAccess().canReadBatchUpdates(), "batch-updates", attrs)
        .map(da -> da.createBatch(pipeline, attrList, startStamp, endStamp))
        .reduce((left, right) -> Union.of(left, right).output())
        .orElseThrow(failEmpty());
  }

  /**
   * Create {@link PCollection} from snapshot of given attributes.
   * The snapshot is either read from available storage or
   * created by reduction of updates.
   * @param pipeline {@link Pipeline} to create the {@link PCollection} in
   * @param attrs attributes to read snapshot for
   * @return the {@link PCollection}
   */
  public final PCollection<StreamElement> getBatchSnapshot(
      Pipeline pipeline,
      AttributeDescriptor<?>... attrs) {

    return getBatchSnapshot(pipeline, Long.MIN_VALUE, Long.MAX_VALUE, attrs);
  }

  /**
   * Create {@link PCollection} from snapshot of given attributes.
   * The snapshot is either read from available storage or
   * created by reduction of updates.
   * @param pipeline {@link Pipeline} to create the {@link PCollection} in
   * @param fromStamp ignore updates older than this stamp
   * @param untilStamp read only updates older than this timestamp (i.e. if this
   * method was called at the given timestamp)
   * @param attrs attributes to read snapshot for
   * @return the {@link PCollection}
   */
  public final PCollection<StreamElement> getBatchSnapshot(
      Pipeline pipeline,
      long fromStamp,
      long untilStamp,
      AttributeDescriptor<?>... attrs) {

    List<AttributeDescriptor<?>> attrList = Arrays.stream(attrs)
        .collect(Collectors.toList());

    List<Pair<AttributeDescriptor, Optional<AttributeFamilyDescriptor>>> resolvedAttrs;
    resolvedAttrs = findSuitableFamilies(
        af -> af.getAccess().canReadBatchSnapshot(), attrs)
        .collect(Collectors.toList());

    boolean unresolved = resolvedAttrs.stream()
        .anyMatch(p -> !p.getSecond().isPresent());

    if (!unresolved) {
      return resolvedAttrs.stream()
          .map(p -> p.getSecond().get())
          .map(this::accessorFor)
          .distinct()
          .map(a -> a.createBatch(pipeline, attrList, fromStamp, untilStamp))
          .reduce((left, right) -> Union.of(left, right).output())
          .orElseThrow(failEmpty());
    }
    return PCollectionTools.reduceAsSnapshot(
        getBatchUpdates(pipeline, fromStamp, untilStamp, attrs));
  }

  @SuppressWarnings("unchecked")
  private Stream<DataAccessor> findSuitableAccessors(
      Predicate<AttributeFamilyDescriptor> predicate,
      String accessorType,
      AttributeDescriptor<?>[] attrs) {

    return findSuitableFamilies(predicate, attrs)
        .map(p -> {
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
  private Stream<Pair<AttributeDescriptor, Optional<AttributeFamilyDescriptor>>>
      findSuitableFamilies(
          Predicate<AttributeFamilyDescriptor> predicate,
          AttributeDescriptor<?>[] attrs) {

    return Arrays
        .stream(attrs)
        .map(desc -> Pair.of(desc, repo.getFamiliesForAttribute(desc)
                .stream()
                .filter(predicate)
                // primary families have precedence
                . min(Comparator.comparingInt(a -> a.getType().ordinal()))));
  }

  private DataAccessor accessorFor(AttributeFamilyDescriptor family) {

    if (family.isProxy()) {
      AttributeFamilyProxyDescriptor proxy = family.toProxy();
      DataAccessor readAccessor = accessorFor(proxy.getTargetFamilyRead());
      DataAccessor writeAccessor = accessorFor(proxy.getTargetFamilyWrite());
      return AttributeFamilyProxyDataAccessor.of(
          proxy, readAccessor, writeAccessor);
    }
    URI uri = family.getStorageUri();
    return loader.findForUri(uri)
        .map(f -> f.createAccessor(this, family.getEntity(), uri, family.getCfg()))
        .orElseThrow(() -> new IllegalStateException(
            "No accessor for URI " + family.getStorageUri()));
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

  private static Supplier<IllegalArgumentException> failEmpty() {
    return () -> new IllegalArgumentException("Pass non empty attribute list");
  }

}
