/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage;
import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.direct.core.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.batch.BatchLogObserver.OnNextContext;
import cz.o2.proxima.direct.core.batch.BatchLogObservers;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.batch.ObserveHandle;
import cz.o2.proxima.direct.core.batch.TerminationContext;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcBatchLogReader extends AbstractStorage implements BatchLogReader {

  private final JdbcDataAccessor accessor;
  private final SqlStatementFactory sqlStatementFactory;
  private final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory;

  public JdbcBatchLogReader(
      JdbcDataAccessor accessor,
      SqlStatementFactory sqlStatementFactory,
      EntityDescriptor entityDescriptor,
      URI uri,
      cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory) {

    super(entityDescriptor, uri);
    this.accessor = accessor;
    this.sqlStatementFactory = sqlStatementFactory;
    this.executorFactory = executorFactory;
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    return Collections.singletonList(Partition.of(0));
  }

  @Override
  public ObserveHandle observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    TerminationContext context = new TerminationContext(observer);
    ExecutorService executor = executorFactory.apply();
    Preconditions.checkArgument(
        attributes.stream().noneMatch(AttributeDescriptor::isWildcard),
        "Only non-wildcard attributes supported currently. Got [%s]",
        attributes);
    executor.submit(() -> flushAttributes(attributes, observer, context));
    return context;
  }

  private void flushAttributes(
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer,
      TerminationContext context) {

    Converter<?> converter = accessor.getResultConverter();
    OnNextContext onNextContext = BatchLogObservers.defaultContext(Partition.of(0));
    long stamp = System.currentTimeMillis();
    HikariDataSource source = accessor.borrowDataSource();
    try (PreparedStatement scan = sqlStatementFactory.scanAll(source);
        ResultSet result = scan.executeQuery()) {

      while (!context.isCancelled() && result.next()) {
        for (KeyValue<?> kv :
            converter.asKeyValues(getEntityDescriptor(), attributes, stamp, result)) {
          if (!observer.onNext(kv, onNextContext)) {
            context.cancel();
            break;
          }
        }
      }
      context.finished();
    } catch (Throwable e) {
      context.handleErrorCaught(e, () -> flushAttributes(attributes, observer, context));
    } finally {
      accessor.releaseDataSource();
    }
  }

  @Override
  public Factory<?> asFactory() {
    final JdbcDataAccessor accessor = this.accessor;
    final SqlStatementFactory sqlStatementFactory = this.sqlStatementFactory;
    final EntityDescriptor entityDesc = this.getEntityDescriptor();
    final URI uri = this.getUri();
    final cz.o2.proxima.core.functional.Factory<ExecutorService> executorFactory =
        this.executorFactory;
    return repo ->
        new JdbcBatchLogReader(accessor, sqlStatementFactory, entityDesc, uri, executorFactory);
  }
}
