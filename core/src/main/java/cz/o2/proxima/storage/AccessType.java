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
package cz.o2.proxima.storage;

import com.google.common.collect.Streams;
import cz.o2.proxima.annotations.Stable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/** Access type to {@code attribute family}. */
@Stable
public interface AccessType extends Serializable {

  /**
   * Return {@code AccessType} based on configuration specification.
   *
   * @param spec the specification in `access` field of attribute family
   * @return the {@code AccessType}
   */
  static AccessType from(Object spec) {

    final Set<String> specifiers;
    if (spec instanceof String) {
      specifiers =
          Arrays.stream(spec.toString().split(",")).map(String::trim).collect(Collectors.toSet());
    } else if (spec instanceof Iterable) {
      @SuppressWarnings("unchecked")
      Iterable<Object> iterable = (Iterable<Object>) spec;
      specifiers = Streams.stream(iterable).map(Object::toString).collect(Collectors.toSet());
    } else {
      throw new IllegalArgumentException("Unknown specifier type " + spec.getClass());
    }

    boolean isReadOnly = specifiers.remove("read-only");
    boolean isWriteOnly = specifiers.remove("write-only");
    boolean isReadBatchUpdates = specifiers.remove("batch-updates");
    boolean isReadBatchSnapshot = specifiers.remove("batch-snapshot");
    boolean isReadRandom = specifiers.remove("random-access");
    boolean isReadCommit = specifiers.remove("commit-log");
    boolean isStateCommitLog = specifiers.remove("state-commit-log");
    boolean isListPrimaryKey = specifiers.remove("list-primary-key");
    boolean canCreateCachedView = specifiers.remove("cached-view");

    if (!specifiers.isEmpty()) {
      throw new IllegalArgumentException("Unknown storage tags: " + specifiers);
    }

    return new AccessType() {

      private static final long serialVersionUID = 1L;

      @Override
      public boolean canReadBatchUpdates() {
        return isReadBatchUpdates;
      }

      @Override
      public boolean canReadBatchSnapshot() {
        return isReadBatchSnapshot;
      }

      @Override
      public boolean canRandomRead() {
        return isReadRandom;
      }

      @Override
      public boolean canReadCommitLog() {
        return isStateCommitLog || isReadCommit;
      }

      @Override
      public boolean isReadonly() {
        return isReadOnly;
      }

      @Override
      public boolean isStateCommitLog() {
        return isStateCommitLog;
      }

      @Override
      public boolean isListPrimaryKey() {
        return isListPrimaryKey;
      }

      @Override
      public boolean isWriteOnly() {
        return isWriteOnly;
      }

      @Override
      public boolean canCreateCachedView() {
        return canCreateCachedView;
      }

      @Override
      public String toString() {
        return "AccessType("
            + "canReadBatchUpdates="
            + canReadBatchUpdates()
            + ", canReadBatchSnapshot="
            + canReadBatchSnapshot()
            + ", canRandomRead="
            + canRandomRead()
            + ", canReadCommitLog="
            + canReadCommitLog()
            + ", isReadonly="
            + isReadonly()
            + ", isStateCommitLog="
            + isStateCommitLog()
            + ", isListPrimaryKey="
            + isListPrimaryKey
            + ", isWriteOnly="
            + isWriteOnly
            + ", canCreateCachedView="
            + canCreateCachedView
            + ")";
      }
    };
  }

  static AccessType or(AccessType left, AccessType right) {
    return new AccessType() {

      private static final long serialVersionUID = 1L;

      @Override
      public boolean canReadBatchUpdates() {
        return left.canReadBatchUpdates() || right.canReadBatchUpdates();
      }

      @Override
      public boolean canReadBatchSnapshot() {
        return left.canReadBatchSnapshot() || right.canReadBatchSnapshot();
      }

      @Override
      public boolean canRandomRead() {
        return left.canRandomRead() || right.canRandomRead();
      }

      @Override
      public boolean canReadCommitLog() {
        return left.canReadCommitLog() || right.canReadCommitLog();
      }

      @Override
      public boolean isStateCommitLog() {
        return left.isStateCommitLog() || right.isStateCommitLog();
      }

      @Override
      public boolean isReadonly() {
        return left.isReadonly() || right.isReadonly();
      }

      @Override
      public boolean isListPrimaryKey() {
        return left.isListPrimaryKey() || right.isListPrimaryKey();
      }

      @Override
      public boolean isWriteOnly() {
        return left.isWriteOnly() || right.isWriteOnly();
      }

      @Override
      public boolean canCreateCachedView() {
        return left.canCreateCachedView() || right.canCreateCachedView();
      }
    };
  }

  /**
   * @return {@code true} if this family can be used to access data by batch observing of updates.
   */
  boolean canReadBatchUpdates();

  /** @return {@code true} if this family can be used to access batch snapshot. */
  boolean canReadBatchSnapshot();

  /** @return {@code true} if this family can be used for random reads. */
  boolean canRandomRead();

  /** @return {@code true} if this family can be used for observing the commit log. */
  boolean canReadCommitLog();

  /**
   * @return {@code true} if this family can be used to synthesize batch snapshot from commit log.
   */
  boolean isStateCommitLog();

  /** @return {@code true} if we can we modify the family. */
  boolean isReadonly();

  /** @return {@code true} if this family can access primary key of entities. */
  boolean isListPrimaryKey();

  /** @return {@code true} if this family is accessed only write only */
  boolean isWriteOnly();

  /** @return {@code true} if a cached view can be create from this attribute family */
  boolean canCreateCachedView();
}
