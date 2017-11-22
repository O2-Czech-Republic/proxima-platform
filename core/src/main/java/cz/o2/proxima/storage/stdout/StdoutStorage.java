/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.storage.stdout;

import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractOnlineAttributeWriter;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.StorageDescriptor;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * Dummy storage printing data to stdout.
 */
public class StdoutStorage extends StorageDescriptor {

  public StdoutStorage() {
    super(Arrays.asList("stdout"));
  }

  @Override
  public DataAccessor getAccessor(
      EntityDescriptor entityDesc,
      URI dummy, Map<String, Object> cfg) {

    return new DataAccessor() {

      @Override
      public Optional<AttributeWriterBase> getWriter() {
        return Optional.of(new AbstractOnlineAttributeWriter(entityDesc, dummy) {
          @Override
          public void write(StreamElement data, CommitCallback callback) {
            System.out.println(String.format(
              "Writing entity %s to attribute %s with key %s and value of size %d",
              data.getEntityDescriptor(), data.getAttributeDescriptor(), data.getKey(),
              data.getValue().length));
            callback.commit(true, null);
          }
        });
      }

    };
  }

}
