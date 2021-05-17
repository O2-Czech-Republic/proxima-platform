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
package cz.o2.proxima.direct.core;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.transaction.TransactionalOnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import java.io.Serializable;

/**
 * Writer for attribute values. This is online version, where each element is committed one after
 * another.
 *
 * <p>The ingest process works as follows:
 *
 * <ul>
 *   <li>incoming request is written into {@code CommitLog}, which is instance of this interface
 *   <li>the message is confirmed to the client, because commit log is persistent, durable and
 *       distributed
 *   <li>next, the message is asynchronously consumed by all writes from the commit log and written
 *       to the storages
 * </ul>
 *
 * Note that as a commit log might be marked any "regular" storage of the message. If so, the
 * message is not written to the commit log twice.
 */
@Stable
public interface OnlineAttributeWriter extends AttributeWriterBase {

  /** {@link Serializable} factory for {@link OnlineAttributeWriter}. */
  @Internal
  @FunctionalInterface
  interface Factory<T extends OnlineAttributeWriter> extends AttributeWriterBase.Factory<T> {}

  @Override
  default Type getType() {
    return Type.ONLINE;
  }

  @Override
  default void rollback() {
    // each element is committed online, so there is no need for rollback
  }

  /**
   * Write given serialized attribute value to given entity.
   *
   * @param data the data to write
   * @param statusCallback callback used to commit data processing
   */
  void write(StreamElement data, CommitCallback statusCallback);

  @SuppressWarnings("unchecked")
  @Override
  Factory<? extends OnlineAttributeWriter> asFactory();

  /**
   * @return {@code true} is this is a {@link TransactionalOnlineAttributeWriter}. {@link
   *     TransactionalOnlineAttributeWriter} is used when writing attribute that supports {@link
   *     cz.o2.proxima.repository.TransactionMode} different from {@link
   *     cz.o2.proxima.repository.TransactionMode#NONE}.
   */
  default boolean isTransactional() {
    return false;
  }

  /**
   * @return {@code this} if {@link #isTransactional()} returns true.
   * @throws UnsupportedOperationException if {@link #isTransactional()} returns {@code false}
   */
  default TransactionalOnlineAttributeWriter transactional() {
    throw new UnsupportedOperationException();
  }
}
