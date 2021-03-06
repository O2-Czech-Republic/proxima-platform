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

import com.google.common.collect.Streams;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.repository.DataOperator;
import cz.o2.proxima.repository.Repository;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Loader for various implementations of {@link AbstractDataAccessorFactory}. */
@Internal
public class DataAccessorLoader<
        OP extends DataOperator,
        A extends AbstractDataAccessor,
        T extends AbstractDataAccessorFactory<OP, A>>
    implements Serializable {

  public static <
          OP extends DataOperator,
          A extends AbstractDataAccessor,
          T extends AbstractDataAccessorFactory<OP, A>>
      DataAccessorLoader<OP, A, T> of(Repository repo, Class<T> cls) {

    return new DataAccessorLoader<>(repo, cls);
  }

  private final List<T> loaded;

  private DataAccessorLoader(Repository repo, Class<T> cls) {
    this.loaded = Streams.stream(ServiceLoader.load(cls)).collect(Collectors.toList());
    this.loaded.forEach(f -> f.setup(repo));
  }

  /**
   * Find {@link AbstractDataAccessorFactory} that best handles given URI.
   *
   * @param uri the storage URI to search for
   * @return optional {@link AbstractDataAccessorFactory}.
   */
  public Optional<T> findForUri(URI uri) {
    List<T> acceptConditionally = new ArrayList<>();
    for (T f : loaded) {
      switch (f.accepts(uri)) {
        case ACCEPT:
          return Optional.of(f);
        case ACCEPT_IF_NEEDED:
          acceptConditionally.add(f);
          break;
        default:
          break;
      }
    }
    return acceptConditionally.stream().findAny();
  }
}
