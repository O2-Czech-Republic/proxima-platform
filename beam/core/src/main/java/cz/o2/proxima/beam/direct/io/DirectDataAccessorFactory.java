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
package cz.o2.proxima.beam.direct.io;

import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.DataAccessor;
import cz.o2.proxima.beam.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.net.URI;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link DataAccessorFactory} using {@link DirectDataOperator}. This is fallback implementation
 * that can be used when no native implementation is available.
 */
@Slf4j
public class DirectDataAccessorFactory implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  @Nullable private transient DirectDataOperator direct;

  @Override
  public void setup(Repository repo) {
    direct = repo.hasOperator("direct") ? repo.getOrCreateOperator(DirectDataOperator.class) : null;
  }

  @Override
  public Accept accepts(URI uri) {
    return direct != null && direct.getAccessorFactory(uri).isPresent()
        ? Accept.ACCEPT_IF_NEEDED
        : Accept.REJECT;
  }

  @Override
  public DataAccessor createAccessor(
      BeamDataOperator op, EntityDescriptor entity, URI uri, Map<String, Object> cfg) {

    if (op.hasDirect()) {
      cz.o2.proxima.direct.core.DataAccessor directAccessor =
          op.getDirect()
              .getAccessorFactory(uri)
              .orElseThrow(() -> new IllegalStateException("Missing directLoader?"))
              .createAccessor(op.getDirect(), entity, uri, cfg);
      return new DirectDataAccessorWrapper(
          op.getRepository(), directAccessor, uri, op.getDirect().getContext(), cfg);
    }
    throw new IllegalStateException("Missing direct operator. Cannot create accessor");
  }
}
