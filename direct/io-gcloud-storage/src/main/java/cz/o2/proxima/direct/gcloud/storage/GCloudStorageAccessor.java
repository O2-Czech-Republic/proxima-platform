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
package cz.o2.proxima.direct.gcloud.storage;

import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link DataAccessor} for gcloud storage.
 */
class GCloudStorageAccessor extends AbstractStorage implements DataAccessor {

  final Map<String, Object> cfg;

  public GCloudStorageAccessor(
      EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

    super(entityDesc, uri);
    this.cfg = cfg;
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(new BulkGCloudStorageWriter(
        getEntityDescriptor(), getUri(), cfg, context));
  }

  @Override
  public Optional<BatchLogObservable> getBatchLogObservable(Context context) {
    return Optional.of(new GCloudLogObservable(
        getEntityDescriptor(), getUri(), cfg,
        context::getExecutorService));
  }

}
