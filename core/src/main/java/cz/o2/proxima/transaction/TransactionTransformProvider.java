/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.transaction;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.transform.Transformation;

/**
 * Provider of {@link cz.o2.proxima.transform.Transformation} that is used when transforming {@code
 * _transaction.commit} requests to target families.
 *
 * <p>This transform is likely to be {@link cz.o2.proxima.repository.DataOperator} sensitive, but
 * needs to be created in core and therefore needs a provider (via @AutoService).
 */
@Internal
public interface TransactionTransformProvider {

  /** Create the {@link Transformation} to be used for processing of {@link Commit Commits}. */
  Transformation create();

  /** {@code true} if this provider is intended for tests only. */
  default boolean isTest() {
    return false;
  }
}
