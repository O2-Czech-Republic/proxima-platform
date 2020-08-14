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
package cz.o2.proxima.direct.core;

import com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/** Context created in local instance that can be distributed over wire. */
@Stable
public class Context implements Serializable, ContextProvider {

  private static final long serialVersionUID = 1L;

  /** Resolver of proxima core's attribute family to direct representation. */
  private final UnaryFunction<AttributeFamilyDescriptor, DirectAttributeFamilyDescriptor> resolver;

  /** Executor associated with all asynchronous operations. */
  private final Factory<ExecutorService> executorFactory;

  /** Initialization marker. After deserialization this will be `false`. */
  private transient boolean initialized = false;

  /** Materialized executor. */
  private transient ExecutorService service;

  protected Context(
      UnaryFunction<AttributeFamilyDescriptor, DirectAttributeFamilyDescriptor>
          attributeFamilyResolver,
      Factory<ExecutorService> executorFactory) {

    this.resolver = Objects.requireNonNull(attributeFamilyResolver);
    this.executorFactory = Objects.requireNonNull(executorFactory);
  }

  /**
   * Get executor for asynchronous tasks.
   *
   * @return {@link ExecutorService} to use in runtime
   */
  public ExecutorService getExecutorService() {
    initialize();
    return service;
  }

  @VisibleForTesting
  public Factory<ExecutorService> getExecutorFactory() {
    return executorFactory;
  }

  /**
   * Convert the given {@link AttributeFamilyDescriptor} to {@link DirectAttributeFamilyDescriptor},
   * if possible.
   *
   * @param family the family to convert
   * @return optionally, the converted family
   */
  public Optional<DirectAttributeFamilyDescriptor> resolve(AttributeFamilyDescriptor family) {

    return Optional.ofNullable(resolver.apply(family));
  }

  public DirectAttributeFamilyDescriptor resolveRequired(AttributeFamilyDescriptor family) {

    DirectAttributeFamilyDescriptor result = resolver.apply(family);
    if (result == null) {
      throw new IllegalStateException("Missing direct family descriptor for " + family);
    }
    return result;
  }

  private synchronized void initialize() {
    if (!initialized) {
      service = executorFactory.apply();
      initialized = true;
    }
  }

  @Override
  public Context getContext() {
    return this;
  }
}
