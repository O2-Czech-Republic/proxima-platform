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

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.scheme.ValueSerializer;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;

/** Descriptor of attribute of entity. */
@Internal
public class AttributeDescriptorImpl<T> extends AttributeDescriptorBase<T> {

  private static final long serialVersionUID = 1L;

  private final List<String> transactionalManagerFamilies;

  AttributeDescriptorImpl(
      String name,
      String entity,
      URI schemeUri,
      @Nullable ValueSerializer<T> serializer,
      boolean replica,
      TransactionMode transactionMode,
      List<String> transactionalManagerFamilies) {

    super(name, entity, schemeUri, serializer, replica, transactionMode);
    this.transactionalManagerFamilies = transactionalManagerFamilies;
    Preconditions.checkArgument(
        transactionMode == TransactionMode.NONE || !transactionalManagerFamilies.isEmpty(),
        "Transactional attributes need specification of manager families. Missing for %s.%s",
        entity,
        name);
  }

  @Override
  public String toString() {
    return "AttributeDescriptor(entity=" + entity + ", name=" + name + ")";
  }

  @Override
  public List<String> getTransactionalManagerFamilies() {
    return transactionalManagerFamilies;
  }
}
