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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
  public boolean isExhausted() {
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
  public ListenableFuture<ResultSet> fetchMoreResults() {
    return new ListenableFuture<ResultSet>() {

      @Override
      public void addListener(Runnable r, Executor exctr) {
        r.run();
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }

      @Override
      public ResultSet get() throws InterruptedException, ExecutionException {
        return new EmptyResultSet();
      }

      @Override
      public ResultSet get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return new EmptyResultSet();
      }
    };
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
  public List<ExecutionInfo> getAllExecutionInfo() {
    return Collections.emptyList();
  }
}
