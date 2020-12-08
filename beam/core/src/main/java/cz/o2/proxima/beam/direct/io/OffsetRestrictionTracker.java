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
package cz.o2.proxima.beam.direct.io;

import com.google.common.base.Preconditions;
import cz.o2.proxima.beam.direct.io.OffsetRestrictionTracker.OffsetRange;
import cz.o2.proxima.direct.commitlog.Offset;
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
     * @return initial restriction for {@link
     *     org.apache.beam.sdk.transforms.DoFn.GetInitialRestriction}.
     * @param limit total limit to read
     */
    public static OffsetRange initialRestriction(long limit) {
      return new OffsetRange(limit);
    }

    /** @return restriction that reads from given offset (inclusive) */
    public static OffsetRange startingFrom(Offset start, long limit) {
      return new OffsetRange(start, limit, 0, true);
    }

    @Getter @Nullable private final Offset startOffset;
    @Getter private final boolean startInclusive;

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

    private OffsetRange(@Nullable Offset start, long totalLimit, long consumed, boolean inclusive) {

      this.startOffset = start;
      this.totalLimit = totalLimit;
      this.consumed = consumed;
      this.startInclusive = inclusive;
      extensible = true;
    }

    private OffsetRange(long limit) {
      this(null, limit, 0, false);
    }

    private OffsetRange(Offset startExclusive, long totalLimit, long consumed) {
      this(Objects.requireNonNull(startExclusive), totalLimit, consumed, false);
    }

    public OffsetRange(
        Offset startOffsetExclusive, Offset endOffsetInclusive, long totalLimit, long consumed) {

      this.startOffset = Objects.requireNonNull(startOffsetExclusive);
      this.startInclusive = false;
      this.endOffsetInclusive = Objects.requireNonNull(endOffsetInclusive);
      this.extensible = false;
      this.totalLimit = totalLimit;
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

    public boolean isLimitConsumed() {
      return totalLimit <= consumed;
    }

    /** @return unmodifiable already processed split of the restriction */
    public OffsetRange asPrimary() {
      return new OffsetRange(startOffset, endOffsetInclusive, totalLimit, totalLimit - consumed);
    }

    /**
     * @return residual of not-yet processed work Note that, at all times the primary + residual
     *     should be equivalent to the original (unsplit) range.
     */
    public OffsetRange asResidual() {
      return new OffsetRange(endOffsetInclusive, totalLimit, consumed);
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
