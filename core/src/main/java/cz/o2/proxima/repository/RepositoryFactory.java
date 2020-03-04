/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import java.io.Serializable;

/** Factory for {@link cz.o2.proxima.repository.Repository}. */
@FunctionalInterface
public interface RepositoryFactory extends Serializable {

  class Caching implements RepositoryFactory {

    private static final long serialVersionUID = 1L;

    private static byte initialized = 0;
    private static Repository repo;

    private final RepositoryFactory underlying;

    private Caching(RepositoryFactory underlying) {
      this.underlying = underlying;
    }

    @Override
    public Repository apply() {
      if (initialized == 0) {
        synchronized (Repository.class) {
          if (initialized == 0) {
            repo = underlying.apply();
            initialized = 1;
          }
        }
      }
      return repo;
    }

    void drop() {
      synchronized (Repository.class) {
        repo = null;
        initialized = 0;
      }
    }
  }

  static RepositoryFactory caching(RepositoryFactory factory) {
    return new Caching(factory);
  }

  /**
   * Create new {@link cz.o2.proxima.repository.Repository}
   *
   * @return new repository
   */
  Repository apply();
}
