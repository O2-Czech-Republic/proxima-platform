/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.storage;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.repository.EntityDescriptor;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import lombok.Getter;

/** A class that is super type of all data accessors. */
@Internal
public class AbstractStorage {

  /** An {@link AbstractStorage} that is legitimately {@link Serializable}. */
  public static class SerializableAbstractStorage extends AbstractStorage implements Serializable {

    private static final long serialVersionUID = 1L;

    public SerializableAbstractStorage() {
      // for Serializable
    }

    protected SerializableAbstractStorage(EntityDescriptor entityDesc, URI uri) {
      super(entityDesc, uri);
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
      oos.writeObject(entityDescriptor);
      oos.writeObject(uri);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
      this.entityDescriptor = (EntityDescriptor) ois.readObject();
      this.uri = (URI) ois.readObject();
    }
  }

  /** The entity this writer is created for. */
  @Getter EntityDescriptor entityDescriptor;

  @Getter URI uri;

  public AbstractStorage() {
    // for Serializable
  }

  protected AbstractStorage(EntityDescriptor entityDesc, URI uri) {
    this.entityDescriptor = entityDesc;
    this.uri = uri;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AbstractStorage) {
      AbstractStorage other = (AbstractStorage) obj;
      return other.getUri().equals(uri);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return uri.hashCode();
  }
}
