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
package cz.o2.proxima.repository;

/** Type of transactions of a {@link AttributeDescriptor}. */
public enum TransactionMode {

  /** No transactional support. */
  NONE,

  /**
   * Transactional support within the same attribute (i.e. colliding writes to the same attribute
   * are rejected).
   */
  ATTRIBUTE,

  /**
   * Transactional support within the same entity key (i.e. transaction can contain only single
   * entity key and only single entity type). Note: all attributes within specified transaction
   * *must* have *at least* mode set to ENTITY, otherwise {@link IllegalArgumentException} is
   * thrown. The actual mode of each transaction is defined to be the maximal transaction mode from
   * all attributes *written* by the transaction.
   */
  ENTITY,

  /**
   * Transactional support across different keys of either the same or different entities. Note: all
   * attributes within specified transaction *must* have the transaction type set to ALL, otherwise
   * {@link IllegalArgumentException} will be thrown.
   */
  ALL
}
