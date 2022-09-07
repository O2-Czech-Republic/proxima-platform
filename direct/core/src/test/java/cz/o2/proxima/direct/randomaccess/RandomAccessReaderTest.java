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
package cz.o2.proxima.direct.randomaccess;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader.GetRequest;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.junit.Test;

public class RandomAccessReaderTest {

  final Repository repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  final EntityDescriptor gateway = repo.getEntity("gateway");
  final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");

  @Test
  public void testMultiFetch() {
    Map<String, KeyValue<?>> results =
        ImmutableMap.<String, KeyValue<?>>builder()
            .put(
                "key",
                KeyValue.of(
                    gateway,
                    status,
                    "key",
                    status.getName(),
                    mock(RandomOffset.class),
                    new byte[] {}))
            .build();
    RandomAccessReader reader =
        new RandomAccessReader() {
          @Override
          public RandomOffset fetchOffset(Listing type, String key) {
            return mock(RandomOffset.class);
          }

          @SuppressWarnings("unchecked")
          @Override
          public <T> Optional<KeyValue<T>> get(
              String key, String attribute, AttributeDescriptor<T> desc, long stamp) {

            return Optional.ofNullable((KeyValue<T>) results.get(key));
          }

          @Override
          public void scanWildcardAll(
              String key,
              @Nullable RandomOffset offset,
              long stamp,
              int limit,
              Consumer<KeyValue<?>> consumer) {}

          @Override
          public <T> void scanWildcard(
              String key,
              AttributeDescriptor<T> wildcard,
              @Nullable RandomOffset offset,
              long stamp,
              int limit,
              Consumer<KeyValue<T>> consumer) {}

          @Override
          public void listEntities(
              @Nullable RandomOffset offset,
              int limit,
              Consumer<Pair<RandomOffset, String>> consumer) {}

          @Override
          public EntityDescriptor getEntityDescriptor() {
            return gateway;
          }

          @Override
          public Factory<?> asFactory() {
            return repo -> this;
          }

          @Override
          public void close() {}
        };
    List<Pair<GetRequest<?>, Optional<KeyValue<?>>>> fetched = new ArrayList<>();
    reader
        .multiFetch()
        .get(GetRequest.of("key", status.getName(), status, System.currentTimeMillis()))
        .get(GetRequest.of("key2", status.getName(), status, System.currentTimeMillis()))
        .fetch(fetched::add);
    assertEquals(2, fetched.size());
    assertEquals("key", fetched.get(0).getFirst().getKey());
    assertEquals(Optional.ofNullable(results.get("key")), fetched.get(0).getSecond());
    assertEquals("key2", fetched.get(1).getFirst().getKey());
    assertEquals(Optional.empty(), fetched.get(1).getSecond());
  }
}
