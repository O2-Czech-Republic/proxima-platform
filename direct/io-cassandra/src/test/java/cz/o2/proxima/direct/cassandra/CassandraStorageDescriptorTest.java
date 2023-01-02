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
package cz.o2.proxima.direct.cassandra;

import static org.junit.Assert.*;

import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.storage.internal.AbstractDataAccessorFactory.Accept;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;
import org.mockito.Mockito;

public class CassandraStorageDescriptorTest {
  private final DataAccessorFactory factory = new CassandraStorageDescriptor();

  @Test
  public void testAccept() {
    assertEquals(Accept.ACCEPT, factory.accepts(URI.create("cassandra://host:9042/")));
    assertEquals(Accept.REJECT, factory.accepts(URI.create("hbase:///")));
  }

  @Test
  public void testCreateAccessor() {
    AttributeFamilyDescriptor mockFamily = Mockito.mock(AttributeFamilyDescriptor.class);
    Mockito.when(mockFamily.getCfg())
        .thenReturn(
            Collections.singletonMap(
                CassandraDBAccessor.CQL_FACTORY_CFG, DefaultCqlFactory.class.getName()));
    Mockito.when(mockFamily.getStorageUri())
        .thenReturn(URI.create("cassandra://localhost/table?primary=primary"));
    DirectDataOperator mockDirect = Mockito.mock(DirectDataOperator.class);

    assertNotNull(factory.createAccessor(mockDirect, mockFamily));
  }
}
