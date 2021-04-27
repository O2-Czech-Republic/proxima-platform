/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.transaction;

import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.transaction.Request;
import cz.o2.proxima.transaction.Response;
import cz.o2.proxima.transaction.State;

public interface TransactionManager {

  /**
   * Create a client-side transaction manager.
   *
   * @param direct {@link DirectDataOperator}
   * @return {@link ClientTransactionManager}
   */
  static ClientTransactionManager client(DirectDataOperator direct) {
    return TransactionResourceManager.of(direct);
  }

  /**
   * Create a server-side transaction manager.
   *
   * @param direct {@link DirectDataOperator}
   * @return {@link ServerTransactionManager}
   */
  static ServerTransactionManager server(DirectDataOperator direct) {
    return TransactionResourceManager.of(direct);
  }

  EntityDescriptor getTransaction();

  Wildcard<Request> getRequestDesc();

  Wildcard<Response> getResponseDesc();

  Regular<State> getStateDesc();
}
