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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Test suite for {@link GCloudLogObservableTest}. */
public class GCloudLogObservableTest {

  private final Repository repo = Repository.of(() -> ConfigFactory.load("test-reference.conf"));
  private final EntityDescriptor gateway =
      repo.findEntity("gateway")
          .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
  private final Context context = repo.getOrCreateOperator(DirectDataOperator.class).getContext();

  private GCloudStorageAccessor accessor;

  @Before
  public void setUp() {
    accessor = new GCloudStorageAccessor(gateway, URI.create("gs://dummy"), Collections.emptyMap());
  }

  @Test
  public void testListPartitions() throws URISyntaxException {

    GCloudLogObservable observable =
        new GCloudLogObservable(gateway, accessor, context) {
          @Override
          Storage client() {
            Storage client = mock(Storage.class);
            when(client.list(any(), any()))
                .thenAnswer(
                    new Answer<Page<Blob>>() {

                      @Override
                      public Page<Blob> answer(InvocationOnMock invocation) {
                        return createMockBlobPage();
                      }

                      @SuppressWarnings("unchecked")
                      private Page<Blob> createMockBlobPage() {
                        Page<Blob> ret = mock(Page.class);
                        List<Blob> blobs = createMockBlobs(20);
                        when(ret.iterateAll()).thenReturn(blobs);
                        return ret;
                      }

                      private List<Blob> createMockBlobs(int count) {
                        return IntStream.range(0, count)
                            .mapToObj(
                                i ->
                                    createMockBlob(
                                        "prefix-1234567890000_9876543210000.blob." + i, i))
                            .collect(Collectors.toList());
                      }

                      private Blob createMockBlob(String name, int num) {
                        Blob ret = mock(Blob.class);
                        when(ret.getName()).thenReturn(name);
                        when(ret.getSize()).thenReturn(2L << (num + 6));
                        return ret;
                      }
                    });
            return client;
          }
        };

    assertEquals(2, observable.getPartitions().size());
    assertEquals(1234567890000L, observable.getPartitions().get(0).getMinTimestamp());
    assertEquals(9876543210000L, observable.getPartitions().get(0).getMaxTimestamp());
  }
}
