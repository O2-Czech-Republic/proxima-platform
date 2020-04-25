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
package cz.o2.proxima.direct.gcloud.storage;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link GCloudLogObservableTest}. */
public class GCloudLogObservableTest {

  private final Repository repo = Repository.of(ConfigFactory.load("test-reference.conf"));
  private final EntityDescriptor gateway =
      repo.findEntity("gateway")
          .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
  private final Context context = repo.getOrCreateOperator(DirectDataOperator.class).getContext();

  private GCloudStorageAccessor accessor;

  @Before
  public void setUp() {
    accessor =
        new GCloudStorageAccessor(gateway, URI.create("gs://bucket/path"), Collections.emptyMap());
  }

  @Test
  public void testListPartitions() {

    GCloudLogObservable observable =
        new GCloudLogObservable(accessor, context) {
          @Override
          FileSystem createFileSystem(GCloudStorageAccessor accessor) {
            return new MockGCloudFileSystem(
                accessor.getNamingConvention(), accessor.getRollPeriod()) {
              {
                for (int i = 0; i < 20; i++) {
                  put(1234567890000L + 1200000L * i, new byte[] {}, 2 << (7 + i));
                }
              }
            };
          }
        };

    List<Partition> partitions = observable.getPartitions();
    assertEquals(2, partitions.size());
    assertEquals(1234566000000L, partitions.get(0).getMinTimestamp());
    assertEquals(1234591200000L, partitions.get(0).getMaxTimestamp());
  }
}
