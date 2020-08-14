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

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.commitlog.CommitLogReader.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.Repository;
import java.net.URI;
import javax.annotation.concurrent.NotThreadSafe;

/** Base interface for {@code OnlineAttributeWriter} and {@code BulkAttributeWriter}. */
@Stable
@NotThreadSafe
public interface AttributeWriterBase extends AutoCloseable {

  /**
   * Base interface for factories in implementing classes.
   *
   * @param <W> type parameter
   */
  @FunctionalInterface
  interface Factory<W extends AttributeWriterBase> extends UnaryFunction<Repository, W> {}

  enum Type {
    ONLINE,
    BULK
  }

  /**
   * Retrieve URI of this writer.
   *
   * @return URI of this writer
   */
  URI getUri();

  /**
   * Retrieve type of the writer.
   *
   * @return {@link Type} of the writer
   */
  Type getType();

  /** Rollback the writer to last committed position. */
  void rollback();

  /**
   * Cast this to {@code OnlineAttributeWriter}. This is just a syntactic sugar.
   *
   * @return {@link OnlineAttributeWriter} from this writer
   */
  @SuppressWarnings("unchecked")
  default OnlineAttributeWriter online() {
    return (OnlineAttributeWriter) this;
  }

  /**
   * Case this to {@code BulkAttributeWriter}. This is just a syntactic sugar.
   *
   * @return {@link BulkAttributeWriter} from this writer
   */
  @SuppressWarnings("unchecked")
  default BulkAttributeWriter bulk() {
    return (BulkAttributeWriter) this;
  }

  /** Close allocated resources of this writer. This is supposed to be idempotent. */
  @Override
  void close();

  /**
   * Convert instance of this writer to {@link Factory} suitable for serialization.
   *
   * @return the {@link Factory} representing this reader
   */
  Factory<? extends AttributeWriterBase> asFactory();
}
