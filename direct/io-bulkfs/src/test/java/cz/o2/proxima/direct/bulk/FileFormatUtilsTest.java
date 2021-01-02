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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.util.Pair;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

/** Test {@link cz.o2.proxima.direct.bulk.FileFormatUtils}. */
public class FileFormatUtilsTest {
  private final FileFormat format = FileFormat.blob(true);

  @Test
  public void testNamingConvention() {
    NamingConvention namingConvention =
        FileFormatUtils.getNamingConvention("io", Collections.emptyMap(), 3600000, format);
    assertTrue(namingConvention.nameOf(1500000000000L).startsWith("/2017/"));
  }

  @Test
  public void testNamingConventionWhenCustomNamingConventionFactory() {
    Map<String, Object> cfg =
        Collections.singletonMap(
            "io.naming-convention-factory", DummyNamingConventionFactory.class.getName());
    NamingConvention namingConvention =
        FileFormatUtils.getNamingConvention("io.", cfg, 3600000, format);
    assertEquals("1500000000000", namingConvention.nameOf(1500000000000L));
  }

  @Test
  public void testNamingConventionWhenCustomNamingConvention() {
    Map<String, Object> cfg =
        Collections.singletonMap("io.naming-convention", DummyNamingConvention.class.getName());
    NamingConvention namingConvention =
        FileFormatUtils.getNamingConvention("io.", cfg, 3600000, format);
    assertEquals("1500000000000", namingConvention.nameOf(1500000000000L));
  }

  public static class DummyNamingConventionFactory implements NamingConventionFactory {

    @Override
    public NamingConvention create(
        String xfgPrefix,
        Map<String, Object> cfg,
        Duration rollTimePeriod,
        String prefix,
        String suffix) {
      return new DummyNamingConvention();
    }
  }

  public static class DummyNamingConvention implements NamingConvention {
    @Override
    public String nameOf(long ts) {
      return String.format("%d", ts);
    }

    @Override
    public Collection<String> prefixesOf(long minTs, long maxTs) {
      return null;
    }

    @Override
    public boolean isInRange(String name, long minTs, long maxTs) {
      return false;
    }

    @Override
    public Pair<Long, Long> parseMinMaxTimestamp(String name) {
      return null;
    }
  }
}
