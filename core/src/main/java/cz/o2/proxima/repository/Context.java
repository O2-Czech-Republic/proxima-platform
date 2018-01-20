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
package cz.o2.proxima.repository;

import cz.seznam.euphoria.core.client.functional.VoidFunction;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;

/**
 * Context created in local instance that can be distributed over wire.
 */
public class Context implements Serializable {

  /**
   * Executor associated with all asynchronous operations.
   */
  private final VoidFunction<ExecutorService> executorFactory;

  /**
   * Initialization marker. After deserialization this will be `false`.
   */
  private transient boolean initialized = false;

  /**
   * Materialized executor.
   */
  private transient ExecutorService service;

  protected Context(VoidFunction<ExecutorService> executorFactory) {
    this.executorFactory = executorFactory;
  }

  /**
   * Get executor for asynchronous tasks.
   * @return {@link ExecutorService} to use in runtime
   */
  public ExecutorService getExecutorService() {
    initialize();
    return service;
  }

  private synchronized void initialize() {
    if (!initialized) {
      service = executorFactory.apply();
      initialized = true;
    }
  }

}
