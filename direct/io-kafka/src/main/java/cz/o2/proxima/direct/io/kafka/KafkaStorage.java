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
package cz.o2.proxima.direct.io.kafka;

import com.google.auto.service.AutoService;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.net.URI;

/** Storage using {@code KafkaProducer}. */
@AutoService(DataAccessorFactory.class)
public class KafkaStorage implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator operator, AttributeFamilyDescriptor familyDescriptor) {

    KafkaAccessor accessor =
        new KafkaAccessor(
            familyDescriptor.getEntity(),
            familyDescriptor.getStorageUri(),
            familyDescriptor.getCfg());
    if (accessor.isTopicRegex()) {
      Preconditions.checkArgument(
          familyDescriptor.getAccess().isReadonly(),
          "URI %s specifies topic regex. Associated family %s MUST be read-only",
          familyDescriptor.getStorageUri(),
          familyDescriptor.getName());
    }
    return accessor;
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("kafka") ? Accept.ACCEPT : Accept.REJECT;
  }
}
