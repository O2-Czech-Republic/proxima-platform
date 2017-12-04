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

package cz.o2.proxima.storage;

import cz.o2.proxima.repository.EntityDescriptor;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import lombok.Getter;

/**
 * Storage acting as a bulk in memory storage.
 */
public class InMemBulkStorage extends StorageDescriptor {

  private class Writer extends AbstractBulkAttributeWriter {

    int writtenSinceLastCommit = 0;

    public Writer(EntityDescriptor entityDesc, URI uri) {
      super(entityDesc, uri);
    }

    @Override
    public void write(StreamElement data, CommitCallback statusCallback) {
      // store the data, commit after each 10 elements
      InMemBulkStorage.this.data.put(
          getURI().getPath() + "/" + data.getKey() + "#" + data.getAttribute(),
          data.getValue());
      System.err.println(" *** written " + data);
      if (++writtenSinceLastCommit >= 10) {
        statusCallback.commit(true, null);
        writtenSinceLastCommit = 0;
      }
    }

    @Override
    public void rollback() {
      // nop
    }

  }

  private class InMemBulkAccessor implements DataAccessor {

    private final EntityDescriptor entityDesc;
    private final URI uri;

    InMemBulkAccessor(EntityDescriptor entityDesc, URI uri) {
      this.entityDesc = entityDesc;
      this.uri = uri;
    }

    @Override
    public Optional<AttributeWriterBase> getWriter() {
      return Optional.of(new Writer(entityDesc, uri));
    }

  }

  @Getter
  private final NavigableMap<String, byte[]> data = new TreeMap<>();

  public InMemBulkStorage() {
    super(Collections.singletonList("inmem-bulk"));
  }

  @Override
  public DataAccessor getAccessor(
      EntityDescriptor entityDesc, URI uri,
      Map<String, Object> cfg) {

    return new InMemBulkAccessor(entityDesc, uri);
  }

}
