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
package cz.o2.proxima.direct.transaction;

import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transaction.KeyAttributes;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.Transaction;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionPreconditionFailedException;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter.TransactionValidator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TransactionUniqueConstraintValidator extends TransactionValidator {

  private DirectDataOperator op;
  private RandomAccessReader reader;
  private EntityDescriptor gateway;
  private Regular<Integer> intField;

  @Override
  public void setup(
      Repository repo, DirectDataOperator directDataOperator, Map<String, Object> cfg) {
    op = directDataOperator;
    gateway = repo.getEntity("gateway");
    intField = Regular.of(gateway, gateway.getAttribute("intField"));
  }

  @Override
  public void validate(StreamElement element, Transaction transaction)
      throws TransactionPreconditionFailedException, TransactionRejectedException {

    if (element.getAttributeDescriptor().equals(intField)) {
      Optional<Integer> intValue = intField.valueOf(element);
      if (intValue.isPresent()) {
        List<String> keys = new ArrayList<>();
        reader().listEntities(p -> keys.add(p.getSecond()));
        for (String k : keys) {
          Optional<KeyValue<Integer>> value = reader.get(k, intField);
          if (value.isPresent()) {
            transaction.update(
                Collections.singletonList(KeyAttributes.ofStreamElement(value.get())));
            if (value.get().getParsedRequired().equals(intValue.get())) {
              throw new TransactionPreconditionFailedException(
                  String.format(
                      "Duplicate value %d, first key: %s, duplicate: %s",
                      value.get().getParsedRequired(), k, element.getKey()));
            }
          } else {
            transaction.update(
                Collections.singletonList(KeyAttributes.ofMissingAttribute(gateway, k, intField)));
          }
        }
      }
    }
  }

  @Override
  public void close() throws Exception {}

  private RandomAccessReader reader() {
    if (reader == null) {
      reader = Optionals.get(op.getRandomAccess(intField));
    }
    return reader;
  }
}
