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
package cz.o2.proxima.storage;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.repository.EntityDescriptor;
import java.io.Serializable;
import java.net.URI;
import lombok.Getter;

/** A class that is super type of all data accessors. */
@Internal
public class AbstractStorage implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The entity this writer is created for. */
  @Getter private final EntityDescriptor entityDescriptor;

  private final URI uri;

  protected AbstractStorage(EntityDescriptor entityDesc, URI uri) {
    this.entityDescriptor = entityDesc;
    this.uri = uri;
  }

  public URI getUri() {
    return uri;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AbstractStorage) {
      AbstractStorage other = (AbstractStorage) obj;
      return other.getUri().equals(uri);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return uri.hashCode();
  }
}
