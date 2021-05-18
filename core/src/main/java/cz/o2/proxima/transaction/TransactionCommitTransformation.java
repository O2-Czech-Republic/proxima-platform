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
package cz.o2.proxima.transaction;

import cz.o2.proxima.repository.ConfigConstants;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transform.ElementWiseTransformation;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionCommitTransformation implements ElementWiseTransformation {

  private Regular<Commit> commitDesc;

  @Override
  public void setup(Repository repo, Map<String, Object> cfg) {
    EntityDescriptor transaction = repo.getEntity(ConfigConstants.TRANSACTION_ENTITY);
    commitDesc = Regular.regular(transaction, transaction.getAttribute("commit"));
  }

  @Override
  public int apply(StreamElement input, Collector<StreamElement> collector) {
    if (input.getAttributeDescriptor().equals(commitDesc)) {
      Optional<Commit> commit = commitDesc.valueOf(input);
      if (commit.isPresent()) {
        commit.get().getUpdates().forEach(collector::collect);
        return commit.get().getUpdates().size();
      }
      log.warn("Unparseable value in {}", input);
    }
    return 0;
  }
}
