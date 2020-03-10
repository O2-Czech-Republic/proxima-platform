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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Factory for {@link cz.o2.proxima.repository.Repository}. */
@FunctionalInterface
public interface RepositoryFactory extends Serializable {

  class VersionedCaching implements RepositoryFactory {

    private static final long serialVersionUID = 1L;

    private static long initializedFrom = Long.MIN_VALUE;
    private static Repository repo;

    private final long version = System.currentTimeMillis();
    private final RepositoryFactory underlying;

    private VersionedCaching(RepositoryFactory underlying, Repository created) {
      this.underlying = underlying;
      synchronized (Repository.class) {
        initializedFrom = version;
        repo = created;
      }
    }

    @Override
    public Repository apply() {
      synchronized (Repository.class) {
        if (initializedFrom < version) {
          ConfigRepository.dropCached();
          repo = underlying.apply();
          initializedFrom = version;
        }
      }
      return repo;
    }

    @VisibleForTesting
    static void drop() {
      ConfigRepository.dropCached();
      initializedFrom = Long.MIN_VALUE;
      repo = null;
    }
  }

  class LocalInstance implements RepositoryFactory {

    private static final long serialVersionUID = 1L;

    private static final Map<Integer, Repository> localMap = new ConcurrentHashMap<>();

    private final int hashCode;

    private LocalInstance(Repository repo) {
      this.hashCode = System.identityHashCode(repo);
      Preconditions.checkState(localMap.put(System.identityHashCode(repo), repo) == null);
    }

    @Override
    public Repository apply() {
      return localMap.get(hashCode);
    }
  }

  static RepositoryFactory caching(RepositoryFactory factory) {
    return new VersionedCaching(factory, factory.apply());
  }

  static RepositoryFactory caching(RepositoryFactory factory, Repository current) {
    return new VersionedCaching(factory, current);
  }

  static RepositoryFactory local(Repository repository) {
    return new LocalInstance(repository);
  }

  /**
   * Create new {@link cz.o2.proxima.repository.Repository}
   *
   * @return new repository
   */
  Repository apply();
}
