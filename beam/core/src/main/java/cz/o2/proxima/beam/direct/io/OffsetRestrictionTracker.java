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
import cz.o2.proxima.beam.direct.io.OffsetRestrictionTracker.OffsetRange;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.time.Watermarks;
import java.io.Serializable;
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
public class OffsetRestrictionTracker extends RestrictionTracker<OffsetRange, Offset> {

  @ToString
  public static class OffsetRange
      implements Serializable, HasDefaultTracker<OffsetRange, OffsetRestrictionTracker> {

    private static final long serialVersionUID = 1L;

    /**
     * @param limit total limit to read
     * @param bounded {@code true} if this is bounded restriction
     * @return initial restriction for {@link
     *     org.apache.beam.sdk.transforms.DoFn.GetInitialRestriction}.*
     */
    public static OffsetRange initialRestriction(long limit, boolean bounded) {
      return new OffsetRange(limit, bounded);
    }

    /** @return restriction that reads from given offset (inclusive) */
    public static OffsetRange startingFrom(
        Partition partition, Position position, OffsetRange initialRestriction) {
      return new OffsetRange(
          partition, position, initialRestriction.getTotalLimit(), initialRestriction.isBounded());
    }

    @Nullable private final Partition partition;
    @Getter @Nullable private final Position position;
    @Getter @Nullable private final Offset startOffset;
    @Getter private final boolean startInclusive;
    @Getter private final boolean bounded;

    // maximal number of elements in all (split) ranges
    @Getter private final long totalLimit;

    // total elements consumed in this range
    private long consumed;

    @Getter @Nullable private Offset endOffsetInclusive;

    // can a tryClaim modify the endOffset
    // this signalizes a running range
    @Getter private boolean extensible;

    // true when the range has been all claimed
    private transient boolean finished = false;

    private OffsetRange(Partition partition, Position position, long totalLimit, boolean bounded) {
      this.partition = partition;
      this.position = position;
      this.totalLimit = totalLimit;
      this.startOffset = null;
      this.extensible = true;
      // do not ignore any data when reading from this restriction
      this.startInclusive = true;
      this.bounded = bounded;
    }

    private OffsetRange(
        @Nullable Offset start,
        long totalLimit,
        boolean bounded,
        long consumed,
        boolean inclusive) {

      this.partition = null;
      this.position = null;
      this.startOffset = start;
      this.totalLimit = totalLimit;
      this.bounded = bounded;
      this.consumed = consumed;
      this.startInclusive = inclusive;
      extensible = true;
    }

    private OffsetRange(long limit, boolean bounded) {
      this(null, limit, bounded, 0, false);
    }

    private OffsetRange(Offset startExclusive, long totalLimit, boolean bounded, long consumed) {
      this(Objects.requireNonNull(startExclusive), totalLimit, bounded, consumed, false);
    }

    public OffsetRange(
        Offset startOffsetExclusive,
        Offset endOffsetInclusive,
        long totalLimit,
        boolean bounded,
        long consumed) {

      this.partition = null;
      this.position = null;
      this.startOffset = startOffsetExclusive;
      this.startInclusive = false;
      this.endOffsetInclusive = Objects.requireNonNull(endOffsetInclusive);
      this.extensible = false;
      this.totalLimit = totalLimit;
      this.bounded = bounded;
      this.consumed = consumed;
    }

    boolean claim(@Nonnull Offset offset) {
      if (!extensible) {
        if (finished) {
          return false;
        }
        finished = offset.equals(endOffsetInclusive);
        return true;
      }
      if (totalLimit > consumed) {
        endOffsetInclusive = offset;
        consumed++;
        return true;
      }
      return false;
    }

    public Partition getPartition() {
      if (partition == null) {
        if (startOffset != null) {
          return startOffset.getPartition();
        }
        return null;
      }
      return partition;
    }

    public boolean isLimitConsumed() {
      return totalLimit <= consumed;
    }

    /** @return unmodifiable already processed split of the restriction */
    public OffsetRange asPrimary() {
      return new OffsetRange(
          startOffset, endOffsetInclusive, totalLimit, bounded, totalLimit - consumed);
    }

    /**
     * @return residual of not-yet processed work Note that, at all times the primary + residual
     *     should be equivalent to the original (unsplit) range.
     */
    public OffsetRange asResidual() {
      return new OffsetRange(endOffsetInclusive, totalLimit, bounded, consumed);
    }

    public boolean isInitial() {
      return startOffset == endOffsetInclusive && endOffsetInclusive == null;
    }

    void terminate() {
      this.extensible = false;
      this.finished = true;
    }

    @Override
    public OffsetRestrictionTracker newTracker() {
      Preconditions.checkState(
          !finished, "Tracker can be created only for not-yet split restrictions");
      Preconditions.checkState(
          extensible, "Tracker can be created only for not-yet split restrictions");
      return new OffsetRestrictionTracker(this);
    }

    /**
     * Verify that we can split this restriction. A restriction is not splittable if it has not been
     * yet started - i.e. no offset has been claimed.
     */
    public boolean isSplittable() {
      return endOffsetInclusive != null;
    }

    /**
     * Verify that we have processed the whole restriction.
     *
     * @return {@code true} if watermark has arrived to final instant
     */
    public boolean isFinished() {
      if (isLimitConsumed()) {
        return true;
      }
      if (endOffsetInclusive != null) {
        return endOffsetInclusive.getWatermark() >= Watermarks.MAX_WATERMARK;
      }
      if (startOffset != null) {
        return startOffset.getWatermark() >= Watermarks.MAX_WATERMARK;
      }
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OffsetRange that = (OffsetRange) o;
      return startInclusive == that.startInclusive
          && extensible == that.extensible
          && finished == that.finished
          && Objects.equals(startOffset, that.startOffset)
          && Objects.equals(endOffsetInclusive, that.endOffsetInclusive);
    }

    @Override
    public int hashCode() {
      return Objects.hash(startOffset, startInclusive, endOffsetInclusive, extensible, finished);
    }
  }

  private final OffsetRange currentRestriction;

  private OffsetRestrictionTracker(OffsetRange initial) {
    this.currentRestriction = initial;
  }

  @Override
  public boolean tryClaim(Offset position) {
    return currentRestriction.claim(position);
  }

  @Override
  public OffsetRange currentRestriction() {
    return currentRestriction;
  }

  @Override
  public @Nullable SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
    if (currentRestriction.isFinished()) {
      return null;
    } else if (currentRestriction.isSplittable()) {
      currentRestriction.terminate();
      return SplitResult.of(currentRestriction.asPrimary(), currentRestriction.asResidual());
    }
    return SplitResult.of(null, currentRestriction);
  }

  @Override
  public void checkDone() throws IllegalStateException {
    // nop
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  public Progress getProgress() {
    // FIXME: we need a way to compute size from two Offsets
    return Progress.from(0.0, 0.0);
  }
}
