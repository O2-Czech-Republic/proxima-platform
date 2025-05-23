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
package cz.o2.proxima.direct.transaction.manager;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;

@FunctionalInterface
@Internal
public interface TransactionLogObserverFactory {

  class Default implements TransactionLogObserverFactory {
    @Override
    public TransactionLogObserver create(DirectDataOperator direct, Metrics metrics) {
      return new TransactionLogObserver(direct, metrics);
    }
  }

  class WithOnErrorHandler implements TransactionLogObserverFactory {

    private final Consumer<Throwable> errorHandler;

    WithOnErrorHandler(Consumer<Throwable> errorHandler) {
      this.errorHandler = errorHandler;
    }

    @Override
    public TransactionLogObserver create(DirectDataOperator direct, Metrics metrics) {
      return new TransactionLogObserver(direct, metrics) {
        @Override
        public boolean onError(Throwable error) {
          errorHandler.accept(error);
          return super.onError(error);
        }
      };
    }
  }

  /**
   * A factory for {@link CommitLogObserver} responsible for transaction management.
   *
   * @param direct the direct operator for the observer
   * @param metrics the metrics to use for reporting
   * @return the {@link CommitLogObserver}
   */
  TransactionLogObserver create(DirectDataOperator direct, Metrics metrics);
}
