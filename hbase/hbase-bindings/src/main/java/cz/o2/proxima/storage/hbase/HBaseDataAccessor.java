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
package cz.o2.proxima.storage.hbase;

import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

/**
 * {@code DataAccessor} for HBase.
 */
public class HBaseDataAccessor extends AbstractStorage implements DataAccessor {

  private final Map<String, Object> cfg;

  public HBaseDataAccessor(
      EntityDescriptor entity,
      URI uri,
      Map<String, Object> cfg) {

    super(entity, uri);
    this.cfg = cfg;
  }

  @Override
  public Optional<AttributeWriterBase> getWriter() {
    return Optional.of(new HBaseWriter(getURI(), Util.getConf(getURI()), cfg));
  }

  @Override
  public Optional<RandomAccessReader> getRandomAccessReader() {
    return Optional.of(new RandomHBaseReader(
        getURI(), Util.getConf(getURI()), cfg, getEntityDescriptor()));
  }

  @Override
  public Optional<BatchLogObservable> getBatchLogObservable() {
    return Optional.of(new HBaseLogObservable(
        getURI(), Util.getConf(getURI()), cfg, getEntityDescriptor()));
  }

}
