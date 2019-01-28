/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core;

import cz.o2.proxima.repository.EntityDescriptor;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;

/**
 * Factory for {@link DataAccessor}s from given URI.
 */
public interface DataAccessorFactory extends Serializable {

  /**
   * Check if this factory can create accessors for given URI.
   * @param uri the URI to create accessor for
   * @return {@code true} if the factory can create accessors for the URI
   */
  boolean accepts(URI uri);

  /**
   * Create the accessor for given URI.
   * @param entity the descriptor of entity to create accessor for
   * @param uri the URI to create accessor for
   * @param cfg optional additional configuration
   * @return the accessor
   */
  DataAccessor create(EntityDescriptor entity, URI uri, Map<String, Object> cfg);

}
