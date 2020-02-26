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
package cz.o2.proxima.direct.bulk;

import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import cz.o2.proxima.util.Pair;
import java.time.Duration;
import java.util.Collection;
import org.junit.Test;

public class DefaultNamingConventionTest {

  private final NamingConvention naming =
      new DefaultNamingConvention(Duration.ofHours(2), "prefix", "tmp", () -> "uuid");

  @Test
  public void testPartitionsRange() {
    assertTrue(
        naming.isInRange(
            "prefix-1234567890000_9876543210000.blob.whatever", 1234567890000L, 12345678901000L));
    assertTrue(
        naming.isInRange(
            "prefix-1234567890000_9876543210000.blob", 1234567891000L, 12345678902000L));
    assertTrue(
        naming.isInRange(
            "/my/dummy/path/prefix-1234567890000_9876543210000.blob",
            1234567891000L,
            12345678902000L));
    assertTrue(
        naming.isInRange(
            "/my/dummy/path/prefix-1234567890000_9876543210000_suffix.blob",
            1234567891000L,
            12345678902000L));
    assertTrue(
        naming.isInRange(
            "prefix-1234567890000_9876543210000.blob.whatever", 1234567891000L, 12345678902000L));
    assertTrue(
        naming.isInRange(
            "prefix-1234567890000_9876543210000.blob.whatever", 1234567880000L, 12345678902000L));
    assertTrue(
        naming.isInRange(
            "prefix-1234567890000_9876543210000.blob.whatever", 9876543200000L, 9999999999999L));

    assertFalse(
        naming.isInRange(
            "prefix-1234567890000_9876543210000.blob.whatever", 1234567880000L, 1234567881000L));
    assertFalse(
        naming.isInRange(
            "prefix-1234567890000_9876543210000.blob.whatever", 9999999999000L, 9999999999999L));
  }

  @Test
  public void testConvertStampsToPrefixes() {
    Collection<String> prefixes = naming.prefixesOf(1541022824110L, 1541109235381L);
    assertEquals(Sets.newHashSet("/2018/10", "/2018/11"), prefixes);
  }

  @Test
  public void testNameGenerate() {
    long now = 1500000000000L;
    assertEquals("/2017/07/prefix-1499997600000_1500004800000_uuid.tmp", naming.nameOf(now));
    assertEquals(
        "/2017/07/prefix-1499997600000_1500004800000_uuid.tmp", naming.nameOf(now + 3600000));
    assertTrue(naming.isInRange(naming.nameOf(now), now, now + 1));
    assertTrue(naming.isInRange(naming.nameOf(now), now - 2400000, now - 2400000 + 1));
    assertFalse(naming.isInRange(naming.nameOf(now), now - 2400000 - 1, now - 2400000));
  }

  @Test
  public void testParsingMinMaxStamp() {
    long now = 1500000000000L;
    Pair<Long, Long> parsed = naming.parseMinMaxTimestamp(naming.nameOf(now));
    assertEquals(now - now % 7200000L, parsed.getFirst().longValue());
    assertEquals(now - now % 7200000L + 7200000L, parsed.getSecond().longValue());
  }
}
