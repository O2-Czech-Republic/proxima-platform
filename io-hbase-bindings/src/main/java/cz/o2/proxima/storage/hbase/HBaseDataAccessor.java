/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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

import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * {@code DataAccessor} for HBase.
 */
public class HBaseDataAccessor extends AbstractStorage implements DataAccessor {

  /**
   * Optional function to be used when creating configuration from URI.
   * This can be used to update the configuration with data stored at
   * the specified map.
   */
  public interface ConfFactory
      extends BinaryFunction<Map<String, Object>, URI, Configuration> {

  }

  private final Map<String, Object> cfg;
  private final ConfFactory confFactory;

  public HBaseDataAccessor(
      EntityDescriptor entity,
      URI uri,
      Map<String, Object> cfg) {

    this(entity, uri, cfg, (m, u) -> Util.getConf(u));
  }

  public HBaseDataAccessor(
      EntityDescriptor entity,
      URI uri,
      Map<String, Object> cfg,
      ConfFactory confFactory) {

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
    return Optional.of(new RandomHBaseReader(
        getUri(), getConf(), cfg, getEntityDescriptor()));
  }

  @Override
  public Optional<BatchLogObservable> getBatchLogObservable(Context context) {
    return Optional.of(new HBaseLogObservable(
        getUri(), getConf(), cfg, getEntityDescriptor(),
        () -> context.getExecutorService()));
  }

  private Configuration getConf() {
    return HBaseConfiguration.create(confFactory.apply(cfg, getUri()));
  }

}
