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
package cz.o2.proxima.direct.bigtable;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.hbase.HBaseDataAccessor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.storage.internal.AbstractDataAccessorFactory.Accept;
import java.net.URI;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

public class BigTableStorageTest {
  private final DataAccessorFactory factory = new BigTableStorage();
  private final DirectDataOperator mockDirect = Mockito.mock(DirectDataOperator.class);
  private final AttributeFamilyDescriptor family = Mockito.mock(AttributeFamilyDescriptor.class);
  private final HBaseDataAccessor accessor =
      (HBaseDataAccessor) factory.createAccessor(mockDirect, family);

  @Test
  public void acceptTest() {
    assertEquals(Accept.ACCEPT, factory.accepts(URI.create("bigtable://projectId:instanceId")));
    assertEquals(Accept.REJECT, factory.accepts(URI.create("test:///")));
  }

  @Test
  public void createAccessorForValidUriTest() {
    final URI uri = URI.create("bigtable://projectId:instanceId");
    Mockito.when(family.getStorageUri()).thenReturn(uri);
    final Configuration conf = accessor.getConfFactory().apply(Collections.emptyMap(), uri);
    assertEquals("projectId", conf.get("google.bigtable.project.id"));
    assertEquals("instanceId", conf.get("google.bigtable.instance.id"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAccessorForUriWithMissingAuthority() {
    accessor.getConfFactory().apply(Collections.emptyMap(), URI.create("bigtable:///"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateAccessorForUriWithMissingInstanceId() {
    accessor.getConfFactory().apply(Collections.emptyMap(), URI.create("bigtable://projectId"));
  }
}
