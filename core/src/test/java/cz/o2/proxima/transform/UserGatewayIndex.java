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
package cz.o2.proxima.transform;

import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.Map;
import java.util.UUID;

public class UserGatewayIndex implements ElementWiseTransformation {

  EntityDescriptor user;
  EntityDescriptor gateway;
  private Wildcard<byte[]> userGateways;
  private Wildcard<byte[]> gatewayUsers;

  @Override
  public void setup(Repository repo, Map<String, Object> cfg) {
    user = repo.getEntity("user");
    gateway = repo.getEntity("gateway");
    userGateways = Wildcard.of(user, user.getAttribute("gateway.*"));
    gatewayUsers = Wildcard.of(gateway, gateway.getAttribute("user.*"));
  }

  @Override
  public int apply(StreamElement input, Collector<StreamElement> collector) {
    if (input.getAttributeDescriptor().equals(userGateways)) {
      collector.collect(
          StreamElement.upsert(
              gateway,
              gatewayUsers,
              UUID.randomUUID().toString(),
              userGateways.extractSuffix(input.getAttribute()),
              gatewayUsers.toAttributePrefix() + input.getKey(),
              input.getStamp(),
              input.getValue()));
      return 1;
    } else if (input.getAttributeDescriptor().equals(gatewayUsers)) {
      collector.collect(
          StreamElement.upsert(
              user,
              userGateways,
              UUID.randomUUID().toString(),
              gatewayUsers.extractSuffix(input.getAttribute()),
              userGateways.toAttributePrefix() + input.getKey(),
              input.getStamp(),
              input.getValue()));
      return 1;
    }
    return 0;
  }
}
