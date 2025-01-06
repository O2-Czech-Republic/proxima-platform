/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.hbase;

import cz.o2.proxima.core.functional.BiFunction;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage.SerializableAbstractStorage;
import cz.o2.proxima.core.storage.UriUtil;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * {@code DataAccessor} for HBase.
 *
 * <p>HBase storage uses URIs in the form of {@code
 * hbase://<master_hostport>/<table>?family=<family>} and stores data using HBase table named {@code
 * table} in family {@code family}.
 *
 * <p>An optional parameter in URI called {@code v} can be used to distinguish two serialization
 * versions of data in HBase {@link org.apache.hadoop.hbase.Cell}:
 *
 * <ol>
 *   <li>{@code v=1} (default) stores the serialized bytes of a value in a cell directly
 *   <li>{@code v=2} uses protobuffer to store more metadata into the value
 * </ol>
 *
 * The {@code v=2} serialization format is required to support transactions on top of HBase, because
 * the metadata preserves sequentialId (stored in {@code StreamElement#getSequentialId}).
 */
@Slf4j
public class HBaseDataAccessor extends SerializableAbstractStorage implements DataAccessor {

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

  /**
   * Just for test purpose!
   *
   * @return configuration factory
   */
  @VisibleForTesting
  public ConfFactory getConfFactory() {
    return confFactory;
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

  static InternalSerializer instantiateSerializer(URI uri) {
    Map<String, String> queryAsMap = UriUtil.parseQuery(uri);
    if (Optional.ofNullable(queryAsMap.get("v")).map(Integer::valueOf).orElse(1) == 2) {
      log.info("Using V2Serializer for URI {}", uri);
      return new InternalSerializer.V2Serializer();
    }
    log.info("Using V1Serializer for URI {}", uri);
    return new InternalSerializer.V1Serializer();
  }
}
