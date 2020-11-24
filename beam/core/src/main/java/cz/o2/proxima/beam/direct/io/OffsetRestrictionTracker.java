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
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

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
     */
    public static OffsetRange initialRestriction() {
      return new OffsetRange();
    }

    /** @return restriction that reads from given offset (inclusive) */
    public static OffsetRange startingFrom(Offset start) {
      return new OffsetRange(start, true);
    }

    @Getter @Nullable private final Offset startOffset;
    @Getter private final boolean startInclusive;
    @Getter @Nullable private Offset endOffsetInclusive;

    // can a tryClaim modify the endOffset
    // this signalizes a running range
    @Getter private boolean extensible;

    // true when the range has been all claimed
    private transient boolean finished = false;

    private OffsetRange(@Nullable Offset start, boolean inclusive) {
      this.startOffset = start;
      this.startInclusive = inclusive;
      extensible = true;
    }

    private OffsetRange() {
      this(null, false);
    }

    private OffsetRange(Offset startExclusive) {
      this(Objects.requireNonNull(startExclusive), false);
    }

    public OffsetRange(Offset startOffsetExclusive, Offset endOffsetInclusive) {
      this.startOffset = Objects.requireNonNull(startOffsetExclusive);
      this.startInclusive = false;
      this.endOffsetInclusive = Objects.requireNonNull(endOffsetInclusive);
      this.extensible = false;
    }

    boolean claim(@Nonnull Offset offset) {
      if (!extensible) {
        if (finished) {
          return false;
        }
        finished = offset.equals(endOffsetInclusive);
        return true;
      }
      endOffsetInclusive = offset;
      return true;
    }

    /** @return unmodifiable already processed split of the restriction */
    public OffsetRange asPrimary() {
      return new OffsetRange(startOffset, endOffsetInclusive);
    }

    /**
     * @return residual of not-yet processed work Note that, at all times the primary + residual
     *     should be equivalent to the original (unsplit) range.
     */
    public OffsetRange asResidual() {
      return new OffsetRange(endOffsetInclusive);
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

  /** A {@link WatermarkEstimator} from {@link OffsetRange}. */
  public static class OffsetWatermarkEstimator implements WatermarkEstimator<Void> {

    private final OffsetRange processedRange;

    OffsetWatermarkEstimator(OffsetRange offsetRange) {
      this.processedRange = offsetRange;
    }

    @Override
    public Instant currentWatermark() {
      if (processedRange.getEndOffsetInclusive() != null) {
        return Instant.ofEpochMilli(processedRange.getEndOffsetInclusive().getWatermark());
      } else if (processedRange.getStartOffset() != null) {
        return Instant.ofEpochMilli(processedRange.getStartOffset().getWatermark());
      }
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public Void getState() {
      return null;
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
    if (currentRestriction.isSplittable()) {
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
