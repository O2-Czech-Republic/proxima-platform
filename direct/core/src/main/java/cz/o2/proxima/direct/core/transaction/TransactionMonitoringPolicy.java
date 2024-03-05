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
package cz.o2.proxima.direct.core.transaction;

import cz.o2.proxima.core.transaction.Request;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.State;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Policy that can be specified for monitoring transactions during execution. Implementing classes
 * should take care of performance. This class is required to be thread-safe.
 */
@ThreadSafe
public interface TransactionMonitoringPolicy {

  static TransactionMonitoringPolicy nop() {
    return new TransactionMonitoringPolicy() {
      @Override
      public void incomingRequest(
          String transactionId, Request request, long timestamp, long watermark) {
        // nop
      }

      @Override
      public void stateUpdate(String transactionId, State currentState, @Nullable State newState) {
        // nop
      }

      @Override
      public void outgoingResponse(String transactionId, Response response) {
        // nop
      }
    };
  }

  /**
   * Called when new request regarding given transaction is received.
   *
   * @param transactionId ID of transaction
   * @param request request to be processed
   * @param timestamp timestamp of the request
   * @param watermark watermark of all requests
   */
  void incomingRequest(String transactionId, Request request, long timestamp, long watermark);

  /**
   * Called when transaction state is about to be updated.
   *
   * @param transactionId ID of transaction
   * @param currentState the current state of the transaction
   * @param newState the state to which the transaction will be transitioned
   */
  void stateUpdate(String transactionId, State currentState, @Nullable State newState);

  /**
   * Called when a response to transaction client is about to be sent.
   *
   * @param transactionId ID of transaction
   * @param response the response to be sent
   */
  void outgoingResponse(String transactionId, Response response);
}
