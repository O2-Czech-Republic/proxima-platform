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
package cz.o2.proxima.direct.io.cassandra;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** A result set with no data. */
public class EmptyResultSet implements ResultSet, Serializable {

  @Override
  public Row one() {
    throw new NoSuchElementException();
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    // sorry folks :)
    return null;
  }

  @Override
  public boolean wasApplied() {
    return true;
  }

  @Override
  public boolean isFullyFetched() {
    return true;
  }

  @Override
  public int getAvailableWithoutFetching() {
    return 0;
  }

  @Override
  public List<Row> all() {
    return Collections.emptyList();
  }

  @Override
  public Iterator<Row> iterator() {
    return all().iterator();
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    // sorry folks :)
    return null;
  }

  @Override
  public List<ExecutionInfo> getExecutionInfos() {
    // sorry folks :)
    return List.of();
  }
}
