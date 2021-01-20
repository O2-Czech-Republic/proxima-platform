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
package cz.o2.proxima.beam.direct.io;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import cz.o2.proxima.beam.direct.io.BatchRestrictionTracker.PartitionList;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.Partition;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;

/**
 * A {@link RestrictionTracker} for {@link Offset Offsets} read from {@link
 * cz.o2.proxima.direct.commitlog.CommitLogReader}
 */
public class BatchRestrictionTracker extends RestrictionTracker<PartitionList, Partition> {

  @ToString
  public static class PartitionList
      implements Serializable, HasDefaultTracker<PartitionList, BatchRestrictionTracker> {

    private static final long serialVersionUID = 1L;

    /**
     * @return initial restriction for {@link
     *     org.apache.beam.sdk.transforms.DoFn.GetInitialRestriction}.
     * @param partitions the list of all partitions in {@link BatchLogReader}
     * @param limit total limit to read
     */
    public static PartitionList initialRestriction(List<Partition> partitions, long limit) {
      return new PartitionList(partitions, limit);
    }

    /**
     * Create {@link PartitionList} with given limit containing only single {@link Partition}
     *
     * @param partition the {@link Partition}
     * @param limit total limit
     */
    public static PartitionList ofSinglePartition(Partition partition, long limit) {
      return new PartitionList(Collections.singletonList(partition), limit);
    }

    @Getter private final List<Partition> partitions;

    // maximal number of elements in all (split) ranges
    @Getter private long totalLimit;

    private PartitionList(List<Partition> partition, long totalLimit) {
      this.partitions = Lists.newArrayList(partition);
      this.totalLimit = totalLimit;
    }

    boolean claim(@Nonnull Partition partition) {
      Preconditions.checkState(!partitions.isEmpty());
      Partition first = partitions.remove(0);
      Preconditions.checkState(first.equals(partition));
      return true;
    }

    @Nullable
    public Partition getFirstPartition() {
      if (partitions.isEmpty()) {
        return null;
      }
      return partitions.get(0);
    }

    @Override
    public BatchRestrictionTracker newTracker() {
      return new BatchRestrictionTracker(this);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionList that = (PartitionList) o;
      return Objects.equals(partitions, that.partitions)
          && Objects.equals(totalLimit, that.totalLimit);
    }

    @Override
    public int hashCode() {
      return Objects.hash(partitions, totalLimit);
    }

    public boolean isEmpty() {
      return partitions.isEmpty();
    }

    public void reportConsumed() {
      totalLimit--;
    }

    public boolean isLimitConsumed() {
      return totalLimit <= 0;
    }

    public boolean isFinished() {
      return partitions.isEmpty() || isLimitConsumed();
    }
  }

  private final PartitionList currentRestriction;

  private BatchRestrictionTracker(PartitionList initial) {
    this.currentRestriction = initial;
  }

  @Override
  public boolean tryClaim(Partition partition) {
    return currentRestriction.claim(partition);
  }

  @Override
  public PartitionList currentRestriction() {
    return currentRestriction;
  }

  @Override
  public @Nullable SplitResult<PartitionList> trySplit(double fractionOfRemainder) {
    if (currentRestriction.isEmpty()) {
      return null;
    }
    return SplitResult.of(null, currentRestriction);
  }

  @Override
  public void checkDone() throws IllegalStateException {
    // nop
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }

  public Progress getProgress() {
    // FIXME: we need a way to compute size from two Offsets
    return Progress.from(0.0, 0.0);
  }
}
