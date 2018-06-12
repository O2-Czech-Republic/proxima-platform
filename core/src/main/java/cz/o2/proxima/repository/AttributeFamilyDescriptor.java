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
package cz.o2.proxima.repository;

import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.view.PartitionedCachedView;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.shadow.com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * A family of attributes with the same storage.
 */
@Evolving("Affected by #66")
public class AttributeFamilyDescriptor implements Serializable {

  @Accessors(chain = true)
  public static final class Builder {

    private final List<AttributeDescriptor<?>> attributes = new ArrayList<>();

    @Setter
    private String name;

    @Setter
    private AttributeWriterBase writer;

    @Setter
    private RandomAccessReader randomAccess;

    @Setter
    private CommitLogReader commitLog;

    @Setter
    private BatchLogObservable batchObservable;

    @Setter
    private PartitionedView partitionedView;

    @Setter
    private PartitionedCachedView cachedView;

    @Setter
    private StorageType type;

    @Setter
    private AccessType access;

    @Setter
    private StorageFilter filter = PassthroughFilter.INSTANCE;

    @Setter
    private String source;

    private Builder() { }

    public Builder clearAttributes() {
      attributes.clear();
      return this;
    }

    public Builder addAttribute(AttributeDescriptor<?> desc) {
      attributes.add(desc);
      return this;
    }

    public AttributeFamilyDescriptor build() {
      return new AttributeFamilyDescriptor(
          name, type, attributes, writer, commitLog, batchObservable,
          randomAccess, partitionedView, cachedView, access, filter,
          source);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Getter
  private final String name;

  @Getter
  private final StorageType type;

  private final List<AttributeDescriptor<?>> attributes;

  /**
   * Access type allowed to this family.
   */
  @Getter
  private final AccessType access;

  @Getter
  private final StorageFilter filter;

  /**
   * Writer associated with this attribute family.
   */
  @Nullable
  private final AttributeWriterBase writer;

  @Nullable
  private final CommitLogReader commitLogReader;

  @Nullable
  private final BatchLogObservable batchObservable;

  @Nullable
  private final RandomAccessReader randomAccess;

  @Nullable
  private final PartitionedView partitionedView;

  @Nullable
  private final PartitionedCachedView cachedView;

  @Nullable
  private final String source;

  AttributeFamilyDescriptor(String name,
      StorageType type,
      Collection<AttributeDescriptor<?>> attributes,
      @Nullable AttributeWriterBase writer,
      @Nullable CommitLogReader commitLogReader,
      @Nullable BatchLogObservable batchObservable,
      @Nullable RandomAccessReader randomAccess,
      @Nullable PartitionedView partitionedView,
      @Nullable PartitionedCachedView cachedView,
      AccessType access,
      StorageFilter filter,
      @Nullable String source) {

    this.name = Objects.requireNonNull(name);
    this.type = type;
    this.attributes = Lists.newArrayList(Objects.requireNonNull(attributes));
    this.writer = writer;
    this.commitLogReader = commitLogReader;
    this.batchObservable = batchObservable;
    this.randomAccess = randomAccess;
    this.partitionedView = partitionedView;
    this.cachedView = cachedView;
    this.access = Objects.requireNonNull(access);
    this.filter = filter;
    this.source = source;
  }

  public List<AttributeDescriptor<?>> getAttributes() {
    return Collections.unmodifiableList(attributes);
  }

  @Override
  public String toString() {
    return "AttributeFamily(" + name + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AttributeFamilyDescriptor) {
      AttributeFamilyDescriptor other = (AttributeFamilyDescriptor) obj;
      return other.name.equals(name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  /**
   * Retrieve writer for this family.
   * Empty if this family is not writable.
   * @return optional {@link AttributeWriterBase} of this family
   */
  public Optional<AttributeWriterBase> getWriter() {
    if (!access.isReadonly()) {
      return Optional.of(Objects.requireNonNull(
          writer, "Family " + name + " is not readonly, but has no writer"));
    }
    return Optional.empty();
  }

  /**
   * Retrieve a commit log reader of this family.
   * Empty if this attribute family is not a commit log.
   * @return optional {@link CommitLogReader} of this family
   */
  public Optional<CommitLogReader> getCommitLogReader() {
    if (access.canReadCommitLog()) {
      return Optional.of(Objects.requireNonNull(
          commitLogReader, "Family " + name + " doesn't have commit-log reader"));
    }
    return Optional.empty();
  }

  /**
   * Retrieve batch reader of this family.
   * @return optional {@link BatchLogObservable} of this family
   */
  public Optional<BatchLogObservable> getBatchObservable() {
    if (access.canReadBatchSnapshot() || access.canReadBatchUpdates()) {
      return Optional.of(Objects.requireNonNull(
          batchObservable, "Family " + name + " doesn't have batch observable"));
    }
    return Optional.empty();
  }


  /**
   * Retrieve a random access reader.
   * Empty if this attribute family is not a random access.
   * @return optional {@link RandomAccessReader} of this family
   */
  public Optional<RandomAccessReader> getRandomAccessReader() {
    if (access.canRandomRead()) {
      return Optional.of(Objects.requireNonNull(
          randomAccess, "Family " + name + " doesn't have random access reader"));
    }
    return Optional.empty();
  }

  /**
   * Retrieve a partitioned view.
   * Empty if the attribute family cannot create partitioned view.
   * @return optional {@link PartitionedView} of this family
   */
  public Optional<PartitionedView> getPartitionedView() {
    if (access.canCreatePartitionedView()) {
      return Optional.of(Objects.requireNonNull(
          partitionedView, "Family " + name + " doesn't have partitioned view"));
    }
    return Optional.empty();
  }


  /**
   * Retrieve partitioned cached view.
   * Empty if the attribute family cannot create partitioned cached view.
   * @return optional {@link PartitionedCachedView} of this family
   */
  public Optional<PartitionedCachedView> getPartitionedCachedView() {
    if (access.canCreatePartitionedCachedView()) {
      return Optional.of(Objects.requireNonNull(
          cachedView, "Family " + name + " cannot create cached view"));
    }
    return Optional.empty();
  }

  /**
   * Retrieve optional name of source attribute family, if this is replica.
   * The source might not be explicitly specified (in which case this method
   * returns {@code Optional.empty()} and the source is determined
   * automatically.
   * @return optional specified source family
   */
  public Optional<String> getSource() {
    return Optional.ofNullable(source);
  }

  Builder toBuilder() {
    Builder ret = new Builder()
        .setAccess(access)
        .setBatchObservable(batchObservable)
        .setCachedView(cachedView)
        .setCommitLog(commitLogReader)
        .setFilter(filter)
        .setName(name)
        .setPartitionedView(partitionedView)
        .setRandomAccess(randomAccess)
        .setSource(source)
        .setType(type)
        .setWriter(writer);
    attributes.forEach(ret::addAttribute);
    return ret;
  }

}
