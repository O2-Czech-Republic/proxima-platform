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
package cz.o2.proxima.transform;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.DataOperatorFactory;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.TestDataOperatorFactory;
import cz.o2.proxima.repository.TestDataOperatorFactory.TestDataOperator;
import java.util.Map;
import org.junit.Test;

public class OperatorSpecificTransformationTest {

  public static class Transformation implements ContextualTransformation<TestDataOperator> {

    @Override
    public void setup(Repository repo, TestDataOperator op, Map<String, Object> cfg) {
      assertNotNull(op);
    }

    @Override
    public boolean isDelegateOf(DataOperatorFactory<?> operatorFactory) {
      return operatorFactory instanceof TestDataOperatorFactory;
    }
  }

  public static class Proxy implements ContextualProxyTransform<TestDataOperator> {
    @Override
    public void setup(EntityDescriptor entity, TestDataOperator op) {
      op.setup();
    }

    @Override
    public boolean isDelegateOf(DataOperatorFactory<?> operatorFactory) {
      return operatorFactory instanceof TestDataOperatorFactory;
    }
  }

  @Test
  public void testRepositoryCreate() {
    Repository repo =
        Repository.of(
            ConfigFactory.load("test-operator-transforms.conf")
                .resolve()
                .withFallback(ConfigFactory.load("test-reference.conf").resolve()));
    assertTrue(repo.getTransformations().containsKey("operator-specific"));
    assertTrue(repo.getOrCreateOperator(TestDataOperator.class).isSetupCalled());
    assertTrue(repo.getEntity("gateway").findAttribute("proxied").isPresent());
  }
}
