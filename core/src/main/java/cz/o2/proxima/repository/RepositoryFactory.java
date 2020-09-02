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
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import cz.o2.proxima.util.StringCompressions;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;

/** Factory for {@link cz.o2.proxima.repository.Repository}. */
@FunctionalInterface
public interface RepositoryFactory extends Serializable {

  class Compressed implements RepositoryFactory {

    private static final long serialVersionUID = 1L;

    private final byte[] compressedConfig;

    Compressed(Config config) {
      compressedConfig =
          StringCompressions.gzip(
              config.root().render(ConfigRenderOptions.concise()), StandardCharsets.UTF_8);
    }

    @Override
    public Repository apply() {
      return Repository.of(getConfig());
    }

    Config getConfig() {
      return ConfigFactory.parseString(
          StringCompressions.gunzip(compressedConfig, StandardCharsets.UTF_8));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("compressedConfig.length", compressedConfig.length)
          .toString();
    }
  }

  class VersionedCaching implements RepositoryFactory {

    private static final long serialVersionUID = 1L;

    private static long initializedFrom = Long.MIN_VALUE;
    private static Repository repo;

    @Getter private final long version = System.currentTimeMillis();
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
          repo = ((ConfigRepository) underlying.apply()).withFactory(this);
          initializedFrom = version;
        }
      }
      return repo;
    }

    @VisibleForTesting
    public static void drop() {
      ConfigRepository.dropCached();
      initializedFrom = Long.MIN_VALUE;
      repo = null;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("version", version)
          .add("underlying", underlying)
          .add("classLoader", getClass().getClassLoader())
          .toString();
    }
  }

  class LocalInstance implements RepositoryFactory {

    private static final long serialVersionUID = 1L;

    private static final Map<Integer, Repository> localMap = new ConcurrentHashMap<>();

    static void drop() {
      localMap.clear();
    }

    private final int hashCode;
    private final RepositoryFactory factory;

    private LocalInstance(Repository repo, RepositoryFactory factory) {
      this.hashCode = System.identityHashCode(repo);
      this.factory = factory;
      localMap.put(this.hashCode, repo);
    }

    @Override
    public Repository apply() {
      return localMap.computeIfAbsent(hashCode, k -> factory.apply());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("hashCode", hashCode).toString();
    }
  }

  static RepositoryFactory compressed(Config config) {
    return new Compressed(config);
  }

  static RepositoryFactory caching(RepositoryFactory factory, Repository current) {
    return new VersionedCaching(factory, current);
  }

  static RepositoryFactory local(Repository repository, RepositoryFactory factory) {
    return new LocalInstance(repository, factory);
  }

  /**
   * Create new {@link cz.o2.proxima.repository.Repository}
   *
   * @return new repository
   */
  Repository apply();
}
