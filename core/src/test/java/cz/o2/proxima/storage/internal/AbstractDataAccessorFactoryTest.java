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
package cz.o2.proxima.storage.internal;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.TestDataOperatorFactory.TestDataOperator;
import cz.o2.proxima.storage.internal.AbstractDataAccessorFactory.Accept;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import lombok.EqualsAndHashCode;
import org.junit.Test;

// To make sonar happy...
public class AbstractDataAccessorFactoryTest {

  private final Repository repository =
      Repository.ofTest(ConfigFactory.load("test-reference.conf"));
  private final TestDataAccessorFactory factory = new TestDataAccessorFactory();

  @EqualsAndHashCode
  private static class TestDataAccessorFactory
      implements AbstractDataAccessor,
          AbstractDataAccessorFactory<TestDataOperator, TestDataAccessorFactory> {

    @Override
    public void setup(Repository repo) {}

    @Override
    public Accept accepts(URI uri) {
      return uri.getScheme().equalsIgnoreCase("test") ? Accept.ACCEPT : Accept.REJECT;
    }

    @Override
    public TestDataAccessorFactory createAccessor(
        TestDataOperator operator, EntityDescriptor entity, URI uri, Map<String, Object> cfg) {
      return new TestDataAccessorFactory();
    }

    @Override
    public URI getUri() {
      return URI.create("test:///");
    }
  }

  @Test
  public void contractTest() throws IOException, ClassNotFoundException {
    URI testUri = URI.create("test:///");
    TestUtils.assertSerializable(factory);
    assertEquals(Accept.ACCEPT, factory.accepts(testUri));
    TestDataOperator operator = repository.getOrCreateOperator(TestDataOperator.class);
    TestDataAccessorFactory accessor =
        factory.createAccessor(operator, Optionals.get(repository.getAllFamilies().findAny()));
    accessor.setup(repository);
    assertEquals(Accept.ACCEPT, accessor.accepts(testUri));
    assertEquals(Accept.REJECT, accessor.accepts(URI.create("foo:///")));
  }
}
