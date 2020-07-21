/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
import cz.o2.proxima.direct.core.AttributeWriterBase.Factory;
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
public interface BulkAttributeWriter extends AttributeWriterBase {

  /** {@link Serializable} factory for {@link BulkAttributeWriter}. */
  @Internal
  @FunctionalInterface
  interface Factory<T extends BulkAttributeWriter> extends AttributeWriterBase.Factory<T> {}

  @Override
  default Type getType() {
    return Type.BULK;
  }

  /**
   * Write given serialized attribute value to given entity. Use the statusCallback to commit the
   * whole bulk (of not yet committed elements).
   *
   * @param data the data to writer
   * @param watermark watermark of data being written
   * @param statusCallback callback to commit the data
   */
  void write(StreamElement data, long watermark, CommitCallback statusCallback);

  /**
   * Update watermark when no input data arrives.
   *
   * @param watermark timestamp of the new watermark
   */
  default void updateWatermark(long watermark) {}

  @SuppressWarnings("unchecked")
  @Override
  Factory<?> asFactory();
}
