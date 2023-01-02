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
package cz.o2.proxima.repository;

import com.google.auto.service.AutoService;
import cz.o2.proxima.repository.TestDataOperatorFactory.TestDataOperator;
import lombok.Getter;

/** A {@link DataOperatorFactory} for testing purposes. */
@AutoService(DataOperatorFactory.class)
public class TestDataOperatorFactory implements DataOperatorFactory<TestDataOperator> {

  public static class TestDataOperator implements DataOperator {

    private final Repository repo;
    @Getter private boolean setupCalled = false;

    TestDataOperator(Repository repo) {
      this.repo = repo;
    }

    @Override
    public void close() {
      // nop
    }

    @Override
    public void reload() {
      // nop
    }

    @Override
    public Repository getRepository() {
      return repo;
    }

    public void setup() {
      setupCalled = true;
    }
  }

  @Override
  public String getOperatorName() {
    return "TestDataOperator";
  }

  @Override
  public boolean isOfType(Class<? extends DataOperator> cls) {
    return cls.isAssignableFrom(TestDataOperator.class);
  }

  @Override
  public TestDataOperator create(Repository repo) {
    return new TestDataOperator(repo);
  }
}
