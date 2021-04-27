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
import cz.o2.proxima.storage.StreamElement;
import java.net.URI;

@Internal
public class OnlineAttributeWriters {

  /**
   * A synchronized version of {@link OnlineAttributeWriter}.
   *
   * @param delegate delegating {@link OnlineAttributeWriter}
   * @return the synchronized {@link OnlineAttributeWriter}
   */
  public static OnlineAttributeWriter synchronizedWriter(OnlineAttributeWriter delegate) {
    return new OnlineAttributeWriter() {
      @Override
      public synchronized void write(StreamElement data, CommitCallback statusCallback) {
        delegate.write(data, statusCallback);
      }

      @Override
      public synchronized Factory<? extends OnlineAttributeWriter> asFactory() {
        return delegate.asFactory();
      }

      @Override
      public synchronized URI getUri() {
        return delegate.getUri();
      }

      @Override
      public synchronized void close() {
        delegate.close();
      }

      @Override
      public synchronized void rollback() {
        delegate.rollback();
      }
    };
  }

  private OnlineAttributeWriters() {}
}
