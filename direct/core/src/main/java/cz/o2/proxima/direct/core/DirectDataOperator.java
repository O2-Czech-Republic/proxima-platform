/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core;

import com.google.common.collect.Sets;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.AttributeFamilyProxyDescriptor;
import cz.o2.proxima.repository.DataOperator;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.Repository.Validate;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.internal.DataAccessorLoader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/** {@link DataOperator} implementation for direct accesses. */
@Slf4j
public class DirectDataOperator implements DataOperator, ContextProvider {

  private static Factory<ExecutorService> createExecutorFactory() {
    final AtomicInteger threadId = new AtomicInteger();
    return () ->
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r);
              t.setName(String.format("ProximaRepositoryPool-%d", threadId.incrementAndGet()));
              t.setUncaughtExceptionHandler(
                  (thr, exc) -> log.error("Error running task in thread {}", thr.getName(), exc));
              return t;
            });
  }

  /** Repository. */
  private final Repository repo;

  /** AttributeFamilyDescriptor with associated DirectAttributeFamilyDescriptor. */
  private final Map<AttributeFamilyDescriptor, DirectAttributeFamilyDescriptor> familyMap =
      Collections.synchronizedMap(new HashMap<>());

  /** Cache of writers for all attributes. */
  private final Map<AttributeDescriptor<?>, OnlineAttributeWriter> writers =
      Collections.synchronizedMap(new HashMap<>());

  private Factory<ExecutorService> executorFactory = createExecutorFactory();

  private final Context context;
  private final DataAccessorLoader<DirectDataOperator, DataAccessor, DataAccessorFactory> loader;

  DirectDataOperator(Repository repo) {
    this.repo = repo;
    this.context = new Context(familyMap::get, executorFactory);
    this.loader = DataAccessorLoader.of(repo, DataAccessorFactory.class);
    reload();
  }

  /**
   * Explicitly specify {@link ExecutorService} to use when constructing threads.
   *
   * @param factory the factory for {@link ExecutorService}
   * @return this
   */
  public DirectDataOperator withExecutorFactory(Factory<ExecutorService> factory) {
    this.executorFactory = factory;
    return this;
  }

  @Override
  public final void reload() {
    close();
    familyMap.clear();
    dependencyOrdered(repo.getAllFamilies()).forEach(this::addResolvedFamily);
  }

  /** Create list of families ordered by dependencies between them (non-proxy first). */
  private List<AttributeFamilyDescriptor> dependencyOrdered(
      Stream<AttributeFamilyDescriptor> families) {

    Set<AttributeFamilyDescriptor> available = new HashSet<>();
    List<AttributeFamilyDescriptor> resolved = new ArrayList<>();
    Set<AttributeFamilyDescriptor> toResolve = families.collect(Collectors.toSet());
    while (!toResolve.isEmpty()) {
      // prevent infinite cycles
      List<AttributeFamilyDescriptor> remove = new ArrayList<>();
      List<AttributeFamilyDescriptor> add = new ArrayList<>();
      toResolve
          .stream()
          .filter(af -> !available.contains(af))
          .forEachOrdered(
              af -> {
                if (!af.isProxy()) {
                  available.add(af);
                  resolved.add(af);
                  remove.add(af);
                } else {
                  AttributeFamilyProxyDescriptor proxy = af.toProxy();
                  if (available.contains(proxy.getTargetFamilyRead())
                      && available.contains(proxy.getTargetFamilyWrite())) {

                    available.add(af);
                    resolved.add(af);
                    remove.add(af);
                  } else if (!available.contains(proxy.getTargetFamilyRead())) {
                    add.add(proxy.getTargetFamilyRead());
                  } else {
                    add.add(proxy.getTargetFamilyWrite());
                  }
                }
              });
      if (add.isEmpty() && remove.isEmpty()) {
        throw new IllegalStateException(
            "Cannot make progress in resolving families "
                + toResolve
                    .stream()
                    .map(AttributeFamilyDescriptor::getName)
                    .collect(Collectors.toList())
                + ", currently resolved "
                + available
                    .stream()
                    .map(AttributeFamilyDescriptor::getName)
                    .collect(Collectors.toList()));
      }
      add.forEach(toResolve::add);
      remove.forEach(toResolve::remove);
    }
    return resolved;
  }

  private void addResolvedFamily(AttributeFamilyDescriptor family) {
    try {
      if (!familyMap.containsKey(family)) {
        log.debug("Adding new family {} to familyMap", family);
        if (family.isProxy()) {
          AttributeFamilyProxyDescriptor proxy = family.toProxy();
          familyMap.put(family, DirectAttributeFamilyProxyDescriptor.of(repo, context, proxy));
          addResolvedFamily(proxy.getTargetFamilyRead());
          addResolvedFamily(proxy.getTargetFamilyWrite());
        } else {
          DataAccessor accessor = findFor(family);
          familyMap.put(
              family, new DirectAttributeFamilyDescriptor(repo, family, context, accessor));
        }
      }
    } catch (Exception ex) {
      log.error("Failed to add family {}", family, ex);
      throw ex;
    }
  }

  private DataAccessor findFor(AttributeFamilyDescriptor desc) {
    return loader
        .findForUri(desc.getStorageUri())
        .map(f -> f.createAccessor(this, desc.getEntity(), desc.getStorageUri(), desc.getCfg()))
        .filter(f -> !repo.isShouldValidate(Validate.ACCESSES) || f.isAcceptable(desc))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "No DataAccessor for URI "
                        + desc.getStorageUri()
                        + " found. You might be missing some dependency."));
  }

  /**
   * Retrieve {@link Context} that is used in all distributed operations.
   *
   * @return the serializable context
   */
  @Override
  public Context getContext() {
    return context;
  }

  /**
   * Convert given core family to direct representation.
   *
   * @param family the family to convert
   * @return the converted family
   */
  public DirectAttributeFamilyDescriptor resolveRequired(AttributeFamilyDescriptor family) {

    return context.resolveRequired(family);
  }

  /**
   * Optionally convert given family to direct representation.
   *
   * @param family the family to convert
   * @return the optionally converted family
   */
  public Optional<DirectAttributeFamilyDescriptor> resolve(AttributeFamilyDescriptor family) {

    return context.resolve(family);
  }

  /**
   * Retrieve factory that first matches given uri.
   *
   * @param uri the URI to search factory for
   * @return optional {@link DataAccessorFactory} for specified URI
   */
  public Optional<DataAccessorFactory> getAccessorFactory(URI uri) {
    return loader.findForUri(uri);
  }

  /**
   * Retrieve writer for given {@link AttributeDescriptor}.
   *
   * @param attr the attribute to find writer for
   * @return optional writer
   */
  public Optional<OnlineAttributeWriter> getWriter(AttributeDescriptor<?> attr) {
    synchronized (writers) {
      OnlineAttributeWriter writer = writers.get(attr);
      if (writer == null) {
        repo.getFamiliesForAttribute(attr)
            .stream()
            .filter(af -> af.getType() == StorageType.PRIMARY)
            .filter(af -> !af.getAccess().isReadonly())
            .findAny()
            .flatMap(context::resolve)
            .ifPresent(
                af ->
                    // store writer of this family to all attributes
                    af.getWriter()
                        .ifPresent(
                            w -> af.getAttributes().forEach(a -> writers.put(a, w.online()))));

        return Optional.ofNullable(writers.get(attr));
      }
      return Optional.of(writer);
    }
  }

  /**
   * Retrieve {@link CommitLogReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find commit log reader for
   * @return optional commit log reader
   */
  public Optional<CommitLogReader> getCommitLogReader(Collection<AttributeDescriptor<?>> attrs) {

    return getAccessor(
        attrs,
        a -> a.getDesc().getAccess().canReadCommitLog(),
        DirectAttributeFamilyDescriptor::getCommitLogReader);
  }

  /**
   * Retrieve {@link CommitLogReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find commit log reader for
   * @return optional commit log reader
   */
  @SafeVarargs
  public final Optional<CommitLogReader> getCommitLogReader(AttributeDescriptor<?>... attrs) {

    return getCommitLogReader(Arrays.asList(attrs));
  }

  /**
   * Retrieve {@link CachedView} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find cached view for
   * @return optional cached view
   */
  public Optional<CachedView> getCachedView(Collection<AttributeDescriptor<?>> attrs) {
    return getAccessor(
        attrs,
        a -> a.getDesc().getAccess().canCreateCachedView(),
        DirectAttributeFamilyDescriptor::getCachedView);
  }

  /**
   * Retrieve {@link CachedView} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find cached view for
   * @return optional cached view
   */
  @SafeVarargs
  public final Optional<CachedView> getCachedView(AttributeDescriptor<?>... attrs) {
    return getCachedView(Arrays.asList(attrs));
  }

  /**
   * Retrieve {@link RandomAccessReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find radom access reader for
   * @return optional random access reader
   */
  public Optional<RandomAccessReader> getRandomAccess(Collection<AttributeDescriptor<?>> attrs) {

    return getAccessor(
        attrs,
        a -> a.getDesc().getAccess().canRandomRead(),
        DirectAttributeFamilyDescriptor::getRandomAccessReader);
  }

  /**
   * Retrieve {@link RandomAccessReader} for given {@link AttributeDescriptor}s.
   *
   * @param attrs the attributes to find radom access reader for
   * @return optional random access reader
   */
  @SafeVarargs
  public final Optional<RandomAccessReader> getRandomAccess(AttributeDescriptor<?>... attrs) {

    return getRandomAccess(Arrays.asList(attrs));
  }

  private <T> Optional<T> getAccessor(
      Collection<AttributeDescriptor<?>> attrs,
      UnaryFunction<DirectAttributeFamilyDescriptor, Boolean> mask,
      UnaryFunction<DirectAttributeFamilyDescriptor, Optional<T>> extract) {

    return attrs
        .stream()
        .map(
            a ->
                getFamiliesForAttribute(a).stream().filter(mask::apply).collect(Collectors.toSet()))
        .reduce(Sets::intersection)
        .flatMap(s -> s.stream().findAny())
        .flatMap(extract::apply);
  }

  /** Close the operator and release all allocated resources. */
  @Override
  public void close() {
    synchronized (writers) {
      writers
          .entrySet()
          .stream()
          .map(Map.Entry::getValue)
          .distinct()
          .forEach(OnlineAttributeWriter::close);
      writers.clear();
    }
  }

  /**
   * Resolve all direct attribute representations of given attribute.
   *
   * @param desc descriptor of attribute
   * @return the set of all direct attribute representations
   */
  public Set<DirectAttributeFamilyDescriptor> getFamiliesForAttribute(AttributeDescriptor<?> desc) {

    return repo.getFamiliesForAttribute(desc)
        .stream()
        .map(this::resolveRequired)
        .collect(Collectors.toSet());
  }

  /**
   * Retrieve all families with their direct representation.
   *
   * @return stream of all {@link DirectAttributeFamilyDescriptor}s.
   */
  public Stream<DirectAttributeFamilyDescriptor> getAllFamilies() {
    return repo.getAllFamilies().map(this::resolveRequired);
  }

  @Override
  public Repository getRepository() {
    return repo;
  }
}
