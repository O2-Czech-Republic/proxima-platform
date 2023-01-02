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

import cz.o2.proxima.repository.ConfigConstants;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.KeyPartitioner;
import cz.o2.proxima.storage.commitlog.Partitioner;
import cz.o2.proxima.util.Optionals;

public class TransactionPartitioner implements Partitioner {

  private static final long serialVersionUID = 1L;

  private static final Partitioner OTHER = new KeyPartitioner();

  @Override
  public int getPartitionId(StreamElement element) {
    if (element.getEntityDescriptor().getName().equals(ConfigConstants.TRANSACTION_ENTITY)
        && (element.getAttribute().startsWith(ConfigConstants.RESPONSE_ATTRIBUTE_PREFIX))
        && element.getParsed().isPresent()) {

      return ((Response) Optionals.get(element.getParsed())).getPartitionIdForResponse();
    }

    return OTHER.getPartitionId(element) & Integer.MAX_VALUE;
  }
}
