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
package cz.o2.proxima.direct.hbase;

import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/** {@code DataAccessor} for HBase. */
public class HBaseDataAccessor extends AbstractStorage implements DataAccessor {

  private static final long serialVersionUID = 1L;

  /**
   * Optional function to be used when creating configuration from URI. This can be used to update
   * the configuration with data stored at the specified map.
   */
  public interface ConfFactory extends BiFunction<Map<String, Object>, URI, Configuration> {}

  private final Map<String, Object> cfg;
  private final ConfFactory confFactory;

  public HBaseDataAccessor(EntityDescriptor entity, URI uri, Map<String, Object> cfg) {

    this(entity, uri, cfg, (m, u) -> Util.getConf(u));
  }

  public HBaseDataAccessor(
      EntityDescriptor entity, URI uri, Map<String, Object> cfg, ConfFactory confFactory) {

    super(entity, uri);
    this.cfg = cfg;
    this.confFactory = confFactory;
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(new HBaseWriter(getUri(), getConf(), cfg));
  }

  @Override
  public Optional<RandomAccessReader> getRandomAccessReader(Context context) {
    return Optional.of(new RandomHBaseReader(getUri(), getConf(), cfg, getEntityDescriptor()));
  }

  @Override
  public Optional<BatchLogReader> getBatchLogReader(Context context) {
    return Optional.of(
        new HBaseLogReader(
            getUri(), getConf(), getEntityDescriptor(), context::getExecutorService));
  }

  private Configuration getConf() {
    return HBaseConfiguration.create(confFactory.apply(cfg, getUri()));
  }
}
