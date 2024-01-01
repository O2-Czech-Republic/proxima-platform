/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.jdbc;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.core.storage.internal.AbstractDataAccessorFactory;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Test;

public class JdbcStorageDescriptorTest {

  private final JdbcStorageDescriptor descriptor = new JdbcStorageDescriptor();

  @Test
  public void acceptTest() throws URISyntaxException {
    assertEquals(
        AbstractDataAccessorFactory.Accept.ACCEPT,
        descriptor.accepts(new URI("jdbc://mysql://localhost/test")));
    assertEquals(
        AbstractDataAccessorFactory.Accept.ACCEPT,
        descriptor.accepts(new URI("jdbc://mysql://root:password@localhost/test")));
    assertEquals(
        AbstractDataAccessorFactory.Accept.ACCEPT,
        descriptor.accepts(new URI("JDBC://mysql://localhost/test")));
    assertEquals(
        AbstractDataAccessorFactory.Accept.REJECT,
        descriptor.accepts(new URI("http://localhost:80/foo")));
    assertEquals(
        AbstractDataAccessorFactory.Accept.ACCEPT,
        descriptor.accepts(new URI("jdbc://sqlite:test.db")));
  }
}
