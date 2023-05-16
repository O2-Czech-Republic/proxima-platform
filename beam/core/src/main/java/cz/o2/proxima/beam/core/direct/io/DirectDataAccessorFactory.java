/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core.direct.io;

import com.google.auto.service.AutoService;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.DataAccessor;
import cz.o2.proxima.beam.core.DataAccessorFactory;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.direct.core.DirectDataOperator;
import java.net.URI;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link DataAccessorFactory} using {@link DirectDataOperator}. This is fallback implementation
 * that can be used when no native implementation is available.
 */
@Slf4j
@AutoService(DataAccessorFactory.class)
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
  public DataAccessor createAccessor(BeamDataOperator op, AttributeFamilyDescriptor family) {
    if (op.hasDirect()) {
      cz.o2.proxima.direct.core.DataAccessor directAccessor =
          op.getDirect()
              .getAccessorFactory(family.getStorageUri())
              .orElseThrow(() -> new IllegalStateException("Missing directLoader?"))
              .createAccessor(op.getDirect(), family);
      return new DirectDataAccessorWrapper(
          op.getRepository(), directAccessor, family, op.getDirect().getContext());
    }
    throw new IllegalStateException("Missing direct operator. Cannot create accessor");
  }
}
