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

import cz.o2.proxima.core.transaction.Request;
import cz.o2.proxima.core.transaction.Response;
import cz.o2.proxima.core.transaction.State;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.transaction.TransactionMonitoringPolicy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

public class TestMonitoringPolicy implements TransactionMonitoringPolicy {

  private static List<Request> requests = Collections.synchronizedList(new ArrayList<>());
  private static List<Response> responses = Collections.synchronizedList(new ArrayList<>());
  private static List<Pair<State, State>> states = Collections.synchronizedList(new ArrayList<>());

  public static void clear() {
    requests.clear();
    responses.clear();
    states.clear();
  }

  public static boolean isEmpty() {
    return requests.isEmpty() || responses.isEmpty() || states.isEmpty();
  }

  @Override
  public void incomingRequest(
      String transactionId, Request request, long timestamp, long watermark) {

    requests.add(request);
  }

  @Override
  public void stateUpdate(String transactionId, State currentState, @Nullable State newState) {
    states.add(Pair.of(currentState, newState));
  }

  @Override
  public void outgoingResponse(String transactionId, Response response) {
    responses.add(response);
  }
}
