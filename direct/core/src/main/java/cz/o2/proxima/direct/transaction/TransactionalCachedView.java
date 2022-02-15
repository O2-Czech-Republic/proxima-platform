/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.view.CachedView;
import cz.o2.proxima.repository.TransactionMode;
import cz.o2.proxima.storage.StreamElement;

@Internal
public class TransactionalCachedView extends DelegatingCachedView {

  private final TransactionalOnlineAttributeWriter transactionalWriter;

  public TransactionalCachedView(DirectDataOperator direct, CachedView delegate) {
    super(delegate);
    this.transactionalWriter = TransactionalOnlineAttributeWriter.of(direct, delegate);
  }

  @Override
  public OnlineAttributeWriter online() {
    return this;
  }

  @Override
  public Type getType() {
    return Type.ONLINE;
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    if (data.getAttributeDescriptor().getTransactionMode() != TransactionMode.NONE) {
      transactionalWriter.write(data, statusCallback);
    } else {
      delegate.write(data, statusCallback);
    }
  }

  @Override
  public boolean isTransactional() {
    return true;
  }

  @Override
  public TransactionalOnlineAttributeWriter transactional() {
    return transactionalWriter;
  }

  @Override
  public Factory asFactory() {
    Factory factory = delegate.asFactory();
    return repo ->
        new TransactionalCachedView(
            repo.getOrCreateOperator(DirectDataOperator.class), factory.apply(repo));
  }

  @Override
  public void close() {
    delegate.close();
    transactionalWriter.close();
  }
}
