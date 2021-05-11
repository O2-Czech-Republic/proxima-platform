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
package cz.o2.proxima.scheme;

import cz.o2.proxima.repository.Repository;
import java.net.URI;
import java.util.Optional;
import lombok.Getter;

public class RepositoryInitializedValueSerializer
    implements ValueSerializer<Object>, ValueSerializer.InitializedWithRepository {

  public static class Factory implements ValueSerializerFactory {

    @Override
    public String getAcceptableScheme() {
      return "test-repo-initialized";
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ValueSerializer<T> getValueSerializer(URI specifier) {
      return (ValueSerializer<T>) new RepositoryInitializedValueSerializer();
    }
  }

  @Getter Repository repo = null;

  @Override
  public void setRepository(Repository repository) {
    this.repo = repository;
  }

  @Override
  public Optional<Object> deserialize(byte[] input) {
    return Optional.empty();
  }

  @Override
  public byte[] serialize(Object value) {
    return new byte[0];
  }

  @Override
  public Object getDefault() {
    return new Object();
  }
}
