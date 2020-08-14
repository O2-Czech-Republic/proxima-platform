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
package cz.o2.proxima.direct.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import cz.o2.proxima.direct.core.AbstractOnlineAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/** A {@link OnlineAttributeWriter} implementation for Cassandra. */
@Slf4j
class CassandraWriter extends AbstractOnlineAttributeWriter implements OnlineAttributeWriter {

  private final CassandraDBAccessor accessor;

  CassandraWriter(CassandraDBAccessor accessor) {
    super(accessor.getEntityDescriptor(), accessor.getUri());
    this.accessor = accessor;
  }

  @Override
  public synchronized void write(StreamElement data, CommitCallback statusCallback) {

    try {
      Session session = accessor.ensureSession();
      Optional<BoundStatement> cql = accessor.getCqlFactory().getWriteStatement(data, session);
      if (cql.isPresent()) {
        if (log.isDebugEnabled()) {
          log.debug(
              "Executing statement {} to write {}",
              cql.get().preparedStatement().getQueryString(),
              data);
        }
        accessor.execute(cql.get());
      } else {
        log.warn("Missing CQL statement to write {}. Discarding.", data);
      }
      statusCallback.commit(true, null);
    } catch (Exception ex) {
      log.error("Failed to ingest record {} into cassandra", data, ex);
      // reset the session and cluster connection
      accessor.close();
      statusCallback.commit(false, ex);
    }
  }

  @Override
  public OnlineAttributeWriter.Factory<?> asFactory() {
    final CassandraDBAccessor accessor = this.accessor;
    return repo -> new CassandraWriter(accessor);
  }
}
