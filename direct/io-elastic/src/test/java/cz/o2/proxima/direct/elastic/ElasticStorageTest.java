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
package cz.o2.proxima.direct.elastic;

import static cz.o2.proxima.storage.internal.AbstractDataAccessorFactory.*;
import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import org.junit.jupiter.api.Test;

class ElasticStorageTest {

  @Test
  public void testAccept() {
    ElasticStorage storage = new ElasticStorage();
    assertEquals(Accept.ACCEPT, storage.accepts(URI.create("elastic://asdas")));
    assertEquals(Accept.ACCEPT, storage.accepts(URI.create("elasticsearch://asdas")));
    assertEquals(Accept.REJECT, storage.accepts(URI.create("es://asdas")));
  }
}
