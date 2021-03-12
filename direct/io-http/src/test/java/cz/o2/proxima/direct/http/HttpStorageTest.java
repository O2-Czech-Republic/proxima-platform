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
package cz.o2.proxima.direct.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.storage.internal.AbstractDataAccessorFactory.Accept;
import java.net.URI;
import org.junit.Test;
import org.mockito.Mockito;

public class HttpStorageTest {

  private final DataAccessorFactory factory = new HttpStorage();

  @Test
  public void testAccept() {
    assertEquals(Accept.ACCEPT, factory.accepts(URI.create("https://localhost")));
    assertEquals(Accept.REJECT, factory.accepts(URI.create("file:///dev/null")));
  }

  @Test
  public void testCreateAccessor() {
    final AttributeFamilyDescriptor family = Mockito.mock(AttributeFamilyDescriptor.class);
    assertNotNull(factory.createAccessor(Mockito.mock(DirectDataOperator.class), family));
  }
}
