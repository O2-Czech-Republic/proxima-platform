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

package cz.o2.proxima.repository;

import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.PassthroughFilter;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.view.PartitionedView;
import javax.annotation.Nullable;

/**
 * A family of attributes with the same storage.
 */
public class AttributeFamilyDescriptor {

  public static final class Builder {

    private final List<AttributeDescriptor<?>> attributes = new ArrayList<>();

    @Setter
    @Accessors(chain = true)
    private String name;

    @Setter
    @Accessors(chain = true)
    private AttributeWriterBase writer;

    @Setter
    @Accessors(chain = true)
    private RandomAccessReader randomAccess;

    @Setter
    @Accessors(chain = true)
    private CommitLogReader commitLog;

    @Setter
    @Accessors(chain = true)
    private BatchLogObservable batchObservable;

    @Setter
    @Accessors(chain = true)
    private PartitionedView partitionedView;

    @Setter
    @Accessors(chain = true)
    private StorageType type;

    @Setter
    @Accessors(chain = true)
    private AccessType access;

    @Setter
    @Accessors(chain = true)
    private StorageFilter filter = PassthroughFilter.INSTANCE;

    private Builder() { }

    public Builder addAttribute(AttributeDescriptor<?> desc) {
      attributes.add(desc);
      return this;
    }

    public AttributeFamilyDescriptor build() {
      return new AttributeFamilyDescriptor(
          name, type, attributes, writer, commitLog, batchObservable,
          randomAccess, partitionedView, access, filter);
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

  AttributeFamilyDescriptor(String name,
      StorageType type,
      List<AttributeDescriptor<?>> attributes,
      @Nullable AttributeWriterBase writer,
      @Nullable CommitLogReader commitLogReader,
      @Nullable BatchLogObservable batchObservable,
      @Nullable RandomAccessReader randomAccess,
      @Nullable PartitionedView partitionedView,
      AccessType access,
      StorageFilter filter) {

    this.name = Objects.requireNonNull(name);
    this.type = type;
    this.attributes = Objects.requireNonNull(attributes);
    this.writer = writer;
    this.commitLogReader = commitLogReader;
    this.batchObservable = batchObservable;
    this.randomAccess = randomAccess;
    this.partitionedView = partitionedView;
    this.access = Objects.requireNonNull(access);
    this.filter = filter;
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
   * Empty if this family is not writable
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
   */
  public Optional<PartitionedView> getPartitionedView() {
    if (access.canCreatePartitionedView()) {
      return Optional.of(Objects.requireNonNull(
          partitionedView, "Family " + name + " doesn't have partitioned view"));
    }
    return Optional.empty();
  }

}
