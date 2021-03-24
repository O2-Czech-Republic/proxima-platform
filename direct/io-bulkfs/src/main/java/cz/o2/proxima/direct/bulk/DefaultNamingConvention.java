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
package cz.o2.proxima.direct.bulk;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.util.Pair;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Internal
public class DefaultNamingConvention implements NamingConvention {

  private static final long serialVersionUID = 1L;

  private static final char SEPARATOR = '/';
  private static final Pattern NAME_PATTERN =
      Pattern.compile("[^/]+-([0-9]+)_([0-9]+)[^/]*\\.+[^/]*$");
  private static final DateTimeFormatter DIR_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/");

  private final long rollPeriodMs;
  private final String prefix;
  private final String suffix;
  private final Factory<String> uuidGenerator;

  DefaultNamingConvention(Duration rollPeriod, String prefix, String suffix) {
    this(rollPeriod, prefix, suffix, () -> UUID.randomUUID().toString());
  }

  @VisibleForTesting
  public DefaultNamingConvention(
      Duration rollPeriod, String prefix, String suffix, Factory<String> uuidGenerator) {
    this.rollPeriodMs = rollPeriod.toMillis();
    this.prefix = prefix;
    this.suffix = suffix;
    this.uuidGenerator = uuidGenerator;
  }

  @Override
  public String nameOf(long ts) {
    long boundary = ts - ts % rollPeriodMs;
    String date =
        DIR_FORMAT.format(
            LocalDateTime.ofInstant(
                Instant.ofEpochMilli(boundary), ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
    return String.format(
        "/%s%s-%d_%d_%s.%s",
        date, prefix, boundary, boundary + rollPeriodMs, uuidGenerator.apply(), suffix);
  }

  @Override
  public Set<String> prefixesOf(long startStamp, long endStamp) {
    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy/MM");
    // use TreeSet, so that prefixes are sorted, which will yield
    // partitions roughly sorted by timestamp
    Set<String> prefixes = new TreeSet<>();
    long t = startStamp;
    if (startStamp > Long.MIN_VALUE && endStamp < Long.MAX_VALUE) {
      LocalDateTime time =
          LocalDateTime.ofInstant(Instant.ofEpochMilli(t), ZoneId.ofOffset("UTC", ZoneOffset.UTC));
      LocalDateTime end =
          LocalDateTime.ofInstant(
              Instant.ofEpochMilli(endStamp), ZoneId.ofOffset("UTC", ZoneOffset.UTC));
      while (time.isBefore(end)) {
        prefixes.add(SEPARATOR + format.format(time));
        time = time.plusMonths(1);
      }
      prefixes.add(
          SEPARATOR
              + format.format(
                  LocalDateTime.ofInstant(
                      Instant.ofEpochMilli(endStamp), ZoneId.ofOffset("UTC", ZoneOffset.UTC))));
    } else {
      prefixes.add(String.valueOf(SEPARATOR));
    }
    log.debug("Prefixes of stamp range {}-{} are {}", startStamp, endStamp, prefixes);
    return prefixes;
  }

  @Override
  public boolean isInRange(String name, long minTs, long maxTs) {
    Matcher matcher = getNamePatternMatcherFor(name);
    if (matcher.matches()) {
      long min = Long.parseLong(matcher.group(1));
      long max = Long.parseLong(matcher.group(2));
      return min < maxTs && max > minTs;
    }
    log.warn("Skipping unparseable name {}", name);
    return false;
  }

  @Override
  public Pair<Long, Long> parseMinMaxTimestamp(String name) {
    Matcher matcher = getNamePatternMatcherFor(name);
    if (matcher.matches()) {
      long min = Long.parseLong(matcher.group(1));
      long max = Long.parseLong(matcher.group(2));
      return Pair.of(min, max);
    }
    throw new IllegalArgumentException("Name " + name + " is not understood by this convention.");
  }

  private Matcher getNamePatternMatcherFor(String name) {
    int lastSlash = name.lastIndexOf('/');
    if (lastSlash >= 0) {
      return NAME_PATTERN.matcher(name.substring(lastSlash + 1));
    }
    return NAME_PATTERN.matcher(name);
  }
}
