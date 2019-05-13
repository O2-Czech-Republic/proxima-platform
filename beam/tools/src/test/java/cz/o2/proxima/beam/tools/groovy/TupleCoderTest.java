/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.tools.groovy;

import groovy.lang.Tuple;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test {@link TupleCoder}.
 */
public class TupleCoderTest {

  @Test
  public void testEncodeDecode() throws IOException {
    TupleCoder coder = TupleCoder.of();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Tuple<Object> t1 = new Tuple<>(1, "1", '1');
    coder.encode(t1, baos);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    Tuple<Object> t2 = coder.decode(bais);
    assertEquals(t1, t2);
  }

}
