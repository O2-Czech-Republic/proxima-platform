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

import static org.junit.Assert.*;

import com.google.common.collect.Sets;
import cz.o2.proxima.util.Pair;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DefaultNamingConventionTest {

  @Parameterized.Parameters
  public static Collection<Pair<NamingConvention, String>> parameters() {
    DefaultNamingConvention convention =
        new DefaultNamingConvention(Duration.ofHours(2), "prefix", "tmp", () -> "uuid");
    return Arrays.asList(
        Pair.of(convention, ""),
        Pair.of(NamingConvention.prefixed("/_prefix", convention), "/_prefix"));
  }

  @Parameterized.Parameter public Pair<NamingConvention, String> tested;

  @Test
  public void testPartitionsRange() {
    testPartitionsRange(tested.getFirst(), tested.getSecond());
  }

  @Test
  public void testConvertStampsToPrefixes() {
    testConvertStampsToPrefixes(tested.getFirst(), tested.getSecond());
  }

  @Test
  public void testNameGenerate() {
    testNameGenerate(tested.getFirst(), tested.getSecond());
  }

  @Test
  public void testParsingMinMaxStamp() {
    testParsingMinMaxStamp(tested.getFirst());
  }

  private void testPartitionsRange(NamingConvention naming, String prefix) {
    assertTrue(
        naming.isInRange(
            prefix + "prefix-1234567890000_9876543210000.blob.whatever",
            1234567890000L,
            12345678901000L));
    assertTrue(
        naming.isInRange(
            prefix + "prefix-1234567890000_9876543210000.blob", 1234567891000L, 12345678902000L));
    assertTrue(
        naming.isInRange(
            prefix + "/my/dummy/path/prefix-1234567890000_9876543210000.blob",
            1234567891000L,
            12345678902000L));
    assertTrue(
        naming.isInRange(
            prefix + "/my/dummy/path/prefix-1234567890000_9876543210000_suffix.blob",
            1234567891000L,
            12345678902000L));
    assertTrue(
        naming.isInRange(
            prefix + "prefix-1234567890000_9876543210000.blob.whatever",
            1234567891000L,
            12345678902000L));
    assertTrue(
        naming.isInRange(
            prefix + "prefix-1234567890000_9876543210000.blob.whatever",
            1234567880000L,
            12345678902000L));
    assertTrue(
        naming.isInRange(
            prefix + "prefix-1234567890000_9876543210000.blob.whatever",
            9876543200000L,
            9999999999999L));

    assertFalse(
        naming.isInRange(
            prefix + "prefix-1234567890000_9876543210000.blob.whatever",
            1234567880000L,
            1234567881000L));
    assertFalse(
        naming.isInRange(
            prefix + "prefix-1234567890000_9876543210000.blob.whatever",
            9999999999000L,
            9999999999999L));
  }

  private void testConvertStampsToPrefixes(NamingConvention naming, String prefix) {
    Collection<String> prefixes = naming.prefixesOf(1541022824110L, 1541109235381L);
    assertEquals(
        Sets.newHashSet(prefix + "/2018/10", prefix + "/2018/11"), Sets.newHashSet(prefixes));
  }

  private void testNameGenerate(NamingConvention naming, String prefix) {
    long now = 1500000000000L;
    assertEquals(
        prefix + "/2017/07/prefix-1499997600000_1500004800000_uuid.tmp", naming.nameOf(now));
    assertEquals(
        prefix + "/2017/07/prefix-1499997600000_1500004800000_uuid.tmp",
        naming.nameOf(now + 3600000));
    assertTrue(naming.isInRange(naming.nameOf(now), now, now + 1));
    assertTrue(naming.isInRange(naming.nameOf(now), now - 2400000, now - 2400000 + 1));
    assertFalse(naming.isInRange(naming.nameOf(now), now - 2400000 - 1, now - 2400000));
  }

  private void testParsingMinMaxStamp(NamingConvention naming) {
    long now = 1500000000000L;
    Pair<Long, Long> parsed = naming.parseMinMaxTimestamp(naming.nameOf(now));
    assertEquals(now - now % 7200000L, parsed.getFirst().longValue());
    assertEquals(now - now % 7200000L + 7200000L, parsed.getSecond().longValue());
  }
}
