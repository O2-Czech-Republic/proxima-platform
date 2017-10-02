/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

import cz.o2.proxima.repository.EntityDescriptor;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * Descriptor of storage for attribute family.
 */
public abstract class StorageDescriptor<W extends AttributeWriterBase> implements Serializable {

  /** Schemes of acceptable URIs. */
  @Getter
  private final List<String> acceptableSchemes;

  protected StorageDescriptor(List<String> schemes) {
    this.acceptableSchemes = schemes;
  }

  /**
   * Retrieve accessor to data for the attributes.
   * @param entityDesc the descriptor of entity we construct this writer for
   * @param uri the URI to write to
   * @param cfg additional key-value configuration parameters that might have
   * been passed to the writer
   */
  public DataAccessor<W> getAccessor(
      EntityDescriptor entityDesc,
      URI uri,
      Map<String, Object> cfg) {

    // by default return dummy accessor with no capabilities.
    return new DataAccessor<W>() { };

  }

}
