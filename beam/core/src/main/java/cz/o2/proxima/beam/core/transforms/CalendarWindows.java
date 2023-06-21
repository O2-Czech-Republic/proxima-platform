/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core.transforms;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PartitioningWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;

/** Fork of Beam's CalendarWindows. To be removed after Flink 1.18 runner is implemented. */
public class CalendarWindows {

  private static final Instant DEFAULT_START_DATE = Instant.ofEpochMilli(0);

  /**
   * Returns a {@link WindowFn} that windows elements into periods measured by days.
   *
   * <p>For example, {@code CalendarWindows.days(1)} will window elements into separate windows for
   * each day.
   */
  public static DaysWindows days(int number) {
    return new DaysWindows(number, DEFAULT_START_DATE, TimeZone.getTimeZone("UTC"));
  }

  /**
   * Returns a {@link WindowFn} that windows elements into periods measured by weeks.
   *
   * <p>For example, {@code CalendarWindows.weeks(1, DateTimeConstants.TUESDAY)} will window
   * elements into week-long windows starting on Tuesdays.
   */
  public static DaysWindows weeks(int number, int startDayOfWeek) {
    TimeZone utc = TimeZone.getTimeZone("UTC");
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(DEFAULT_START_DATE, utc.toZoneId());
    int dayOffset =
        DayOfWeek.of(startDayOfWeek).ordinal() - zonedDateTime.getDayOfWeek().ordinal() % 7;
    return new DaysWindows(7 * number, zonedDateTime.plusDays(dayOffset).toInstant(), utc);
  }

  /**
   * Returns a {@link WindowFn} that windows elements into periods measured by months.
   *
   * <p>For example, {@code CalendarWindows.months(8).withStartingMonth(2014, 1).beginningOnDay(10)}
   * will window elements into 8 month windows where that start on the 10th day of month, and the
   * first window begins in January 2014.
   */
  public static MonthsWindows months(int number) {
    return new MonthsWindows(number, 1, DEFAULT_START_DATE, TimeZone.getTimeZone("UTC"));
  }

  /**
   * Returns a {@link WindowFn} that windows elements into periods measured by years.
   *
   * <p>For example, {@code
   * CalendarWindows.years(1).withTimeZone(DateTimeZone.forId("America/Los_Angeles"))} will window
   * elements into year-long windows that start at midnight on Jan 1, in the America/Los_Angeles
   * time zone.
   */
  public static YearsWindows years(int number) {
    TimeZone utc = TimeZone.getTimeZone("UTC");
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(DEFAULT_START_DATE, utc.toZoneId());
    return new YearsWindows(number, 1, 1, zonedDateTime, utc);
  }

  /**
   * A {@link WindowFn} that windows elements into periods measured by days.
   *
   * <p>By default, periods of multiple days are measured starting at the epoch. This can be
   * overridden with {@link #withStartingDay}.
   *
   * <p>The time zone used to determine calendar boundaries is UTC, unless this is overridden with
   * the {@link #withTimeZone} method.
   */
  public static class DaysWindows extends PartitioningWindowFn<Object, IntervalWindow> {
    public DaysWindows withStartingDay(Instant startingDay) {
      return new DaysWindows(number, startingDay, timeZone);
    }

    public DaysWindows withTimeZone(TimeZone timeZone) {
      return new DaysWindows(
          number, startDate.withZoneSameLocal(timeZone.toZoneId()).toInstant(), timeZone);
    }

    ////////////////////////////////////////////////////////////////////////////

    private final int number;
    private final ZonedDateTime startDate;
    private final TimeZone timeZone;

    private DaysWindows(int number, Instant startDate, TimeZone timeZone) {
      this.number = number;
      this.startDate = ZonedDateTime.ofInstant(startDate, timeZone.toZoneId());
      this.timeZone = timeZone;
    }

    @Override
    public IntervalWindow assignWindow(org.joda.time.Instant timestamp) {
      ZonedDateTime datetime =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getMillis()), timeZone.toZoneId());
      long daysBetween = ChronoUnit.DAYS.between(startDate, datetime);
      int dayOffset = (int) (daysBetween / number * number);

      ZonedDateTime begin = startDate.plusDays(dayOffset);
      ZonedDateTime end = begin.plusDays(number);

      return new IntervalWindow(
          new org.joda.time.Instant(begin.toInstant().toEpochMilli()),
          new org.joda.time.Instant(end.toInstant().toEpochMilli()));
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return IntervalWindow.getCoder();
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      if (!(other instanceof DaysWindows)) {
        return false;
      }
      DaysWindows that = (DaysWindows) other;
      return number == that.number
          && Objects.equals(startDate, that.startDate)
          && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "Only %s objects with the same number of days, start date "
                    + "and time zone are compatible.",
                DaysWindows.class.getSimpleName()));
      }
    }

    public int getNumber() {
      return number;
    }

    public ZonedDateTime getStartDate() {
      return startDate;
    }

    public TimeZone getTimeZone() {
      return timeZone;
    }
  }

  /**
   * A {@link WindowFn} that windows elements into periods measured by months.
   *
   * <p>By default, periods of multiple months are measured starting at the epoch. This can be
   * overridden with {@link #withStartingMonth}.
   *
   * <p>Months start on the first day of each calendar month, unless overridden by {@link
   * #beginningOnDay}.
   *
   * <p>The time zone used to determine calendar boundaries is UTC, unless this is overridden with
   * the {@link #withTimeZone} method.
   */
  public static class MonthsWindows extends PartitioningWindowFn<Object, IntervalWindow> {
    public MonthsWindows beginningOnDay(int dayOfMonth) {
      return new MonthsWindows(number, dayOfMonth, startDate.toInstant(), timeZone);
    }

    public MonthsWindows withStartingMonth(int year, int month) {
      ZonedDateTime start =
          ZonedDateTime.of(LocalDate.of(year, month, 1), LocalTime.of(0, 0), timeZone.toZoneId());
      return new MonthsWindows(number, dayOfMonth, start.toInstant(), timeZone);
    }

    public MonthsWindows withTimeZone(TimeZone timeZone) {
      return new MonthsWindows(number, dayOfMonth, startDate.toInstant(), timeZone);
    }

    ////////////////////////////////////////////////////////////////////////////

    private final int number;
    private final int dayOfMonth;
    private final ZonedDateTime startDate;
    private final TimeZone timeZone;

    private MonthsWindows(int number, int dayOfMonth, Instant startDate, TimeZone timeZone) {
      this.number = number;
      this.dayOfMonth = dayOfMonth;
      this.startDate = ZonedDateTime.ofInstant(startDate, timeZone.toZoneId());
      this.timeZone = timeZone;
    }

    @Override
    public IntervalWindow assignWindow(org.joda.time.Instant timestamp) {
      ZonedDateTime datetime =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getMillis()), timeZone.toZoneId());

      int monthOffset =
          (int)
              (ChronoUnit.MONTHS.between(startDate.withDayOfMonth(dayOfMonth), datetime)
                  / number
                  * number);

      ZonedDateTime begin = startDate.withDayOfMonth(dayOfMonth).plusMonths(monthOffset);
      ZonedDateTime end = begin.plusMonths(number);

      return new IntervalWindow(
          new org.joda.time.Instant(begin.toInstant().toEpochMilli()),
          new org.joda.time.Instant(end.toInstant().toEpochMilli()));
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return IntervalWindow.getCoder();
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      if (!(other instanceof MonthsWindows)) {
        return false;
      }
      MonthsWindows that = (MonthsWindows) other;
      return number == that.number
          && dayOfMonth == that.dayOfMonth
          && Objects.equals(startDate, that.startDate)
          && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "Only %s objects with the same number of months, "
                    + "day of month, start date and time zone are compatible.",
                MonthsWindows.class.getSimpleName()));
      }
    }

    public int getNumber() {
      return number;
    }

    public int getDayOfMonth() {
      return dayOfMonth;
    }

    public ZonedDateTime getStartDate() {
      return startDate;
    }

    public TimeZone getTimeZone() {
      return timeZone;
    }
  }

  /**
   * A {@link WindowFn} that windows elements into periods measured by years.
   *
   * <p>By default, periods of multiple years are measured starting at the epoch. This can be
   * overridden with {@link #withStartingYear}.
   *
   * <p>Years start on the first day of each calendar year, unless overridden by {@link
   * #beginningOnDay}.
   *
   * <p>The time zone used to determine calendar boundaries is UTC, unless this is overridden with
   * the {@link #withTimeZone} method.
   */
  public static class YearsWindows extends PartitioningWindowFn<Object, IntervalWindow> {
    public YearsWindows beginningOnDay(int monthOfYear, int dayOfMonth) {
      return new YearsWindows(number, monthOfYear, dayOfMonth, startDate, timeZone);
    }

    public YearsWindows withStartingYear(int year) {
      ZonedDateTime start =
          ZonedDateTime.of(LocalDate.of(year, 1, 1), LocalTime.of(0, 0), timeZone.toZoneId());
      return new YearsWindows(number, monthOfYear, dayOfMonth, start, timeZone);
    }

    public YearsWindows withTimeZone(TimeZone timeZone) {
      return new YearsWindows(
          number,
          monthOfYear,
          dayOfMonth,
          startDate.withZoneSameLocal(timeZone.toZoneId()),
          timeZone);
    }

    ////////////////////////////////////////////////////////////////////////////

    private final int number;
    private final int monthOfYear;
    private final int dayOfMonth;
    private final ZonedDateTime startDate;
    private final TimeZone timeZone;

    private YearsWindows(
        int number, int monthOfYear, int dayOfMonth, ZonedDateTime startDate, TimeZone timeZone) {
      this.number = number;
      this.monthOfYear = monthOfYear;
      this.dayOfMonth = dayOfMonth;
      this.startDate = startDate;
      this.timeZone = timeZone;
    }

    @Override
    public IntervalWindow assignWindow(org.joda.time.Instant timestamp) {
      ZonedDateTime datetime =
          ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp.getMillis()), timeZone.toZoneId());

      ZonedDateTime offsetStart = startDate.withMonth(monthOfYear).withDayOfMonth(dayOfMonth);

      int yearOffset = (int) (ChronoUnit.YEARS.between(offsetStart, datetime) / number * number);

      ZonedDateTime begin = offsetStart.plusYears(yearOffset);
      ZonedDateTime end = begin.plusYears(number);

      return new IntervalWindow(
          new org.joda.time.Instant(begin.toInstant().toEpochMilli()),
          new org.joda.time.Instant(end.toInstant().toEpochMilli()));
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return IntervalWindow.getCoder();
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      if (!(other instanceof YearsWindows)) {
        return false;
      }
      YearsWindows that = (YearsWindows) other;
      return number == that.number
          && monthOfYear == that.monthOfYear
          && dayOfMonth == that.dayOfMonth
          && Objects.equals(startDate, that.startDate)
          && Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "Only %s objects with the same number of years, month of year, "
                    + "day of month, start date and time zone are compatible.",
                YearsWindows.class.getSimpleName()));
      }
    }

    public TimeZone getTimeZone() {
      return timeZone;
    }

    public ZonedDateTime getStartDate() {
      return startDate;
    }

    public int getDayOfMonth() {
      return dayOfMonth;
    }

    public int getMonthOfYear() {
      return monthOfYear;
    }

    public int getNumber() {
      return number;
    }
  }
}
