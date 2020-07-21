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
package cz.o2.proxima.direct.http.opentsdb;

import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.direct.http.ConnFactory;
import cz.o2.proxima.direct.http.HttpWriter;
import cz.o2.proxima.repository.EntityDescriptor;
import java.net.URI;
import java.util.Map;

/** A {@link HttpWriter} specialized on opentsdb. */
@Evolving
public class OpenTsdbWriter extends HttpWriter {

  private static final long serialVersionUID = 1L;

  public OpenTsdbWriter(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {
    super(entityDesc, uri, cfg);
  }

  @Override
  protected ConnFactory getConnFactory(Map<String, Object> cfg) {
    return new OpenTsdbConnectionFactory();
  }

  @Override
  public Factory<?> asFactory() {
    final EntityDescriptor entity = getEntityDescriptor();
    final URI uri = getUri();
    final Map<String, Object> cfg = getCfg();
    return repo -> new OpenTsdbWriter(entity, uri, cfg);
  }
}
