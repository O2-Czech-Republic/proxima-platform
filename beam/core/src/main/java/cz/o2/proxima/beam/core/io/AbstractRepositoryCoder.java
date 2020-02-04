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
package cz.o2.proxima.beam.core.io;

import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import org.apache.beam.sdk.coders.CustomCoder;

abstract class AbstractRepositoryCoder<T> extends CustomCoder<T> {

  private final RepositoryFactory repoFactory;
  private transient Repository repo;

  AbstractRepositoryCoder(RepositoryFactory factory) {
    this.repoFactory = factory;
  }

  Repository repo() {
    if (repo == null) {
      repo = repoFactory.apply();
    }
    return repo;
  }
}
