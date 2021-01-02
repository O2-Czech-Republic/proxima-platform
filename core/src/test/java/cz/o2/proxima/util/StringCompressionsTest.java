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
package cz.o2.proxima.util;

import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Test;

public class StringCompressionsTest {

  @Test
  public void testGzip() {
    final String input = "Super awesome string.";
    final byte[] compressed = StringCompressions.gzip(input, StandardCharsets.UTF_8);
    Assert.assertEquals(input, StringCompressions.gunzip(compressed, StandardCharsets.UTF_8));
  }
}
