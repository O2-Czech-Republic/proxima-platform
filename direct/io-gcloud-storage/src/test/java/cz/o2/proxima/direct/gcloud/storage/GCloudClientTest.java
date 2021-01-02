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
package cz.o2.proxima.direct.gcloud.storage;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.junit.Test;
import org.mockito.stubbing.Answer;

/** Test {@link GCloudClient}. */
public class GCloudClientTest {

  private final Repository repo =
      Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor entity = repo.getEntity("gateway");
  private final URI uri = URI.create("gs://bucket/path");

  @Test
  public void testCreateBlob() {
    createBlob(null);
    createBlob(StorageClass.COLDLINE);
  }

  private void createBlob(@Nullable StorageClass storageClass) {
    Storage storage = mock(Storage.class);
    AtomicReference<BlobInfo> blobInfo = new AtomicReference<>();
    Answer<Blob> answer =
        invocationOnMock -> {
          blobInfo.set((BlobInfo) invocationOnMock.getArguments()[0]);
          return mock(Blob.class);
        };
    when(storage.create((BlobInfo) any(), any())).then(answer);
    when(storage.create((BlobInfo) any())).then(answer);
    GCloudClient client = createClient(storage, storageClass);

    client.createBlob("//my/blob/name");
    assertEquals("path/my/blob/name", blobInfo.get().getName());
    assertEquals("bucket", blobInfo.get().getBucket());
    assertEquals(
        storageClass == null ? StorageClass.STANDARD : storageClass,
        blobInfo.get().getStorageClass());
  }

  private GCloudClient createClient(Storage mock, Map<String, Object> cfg) {
    return new GCloudClient(uri, cfg) {
      @Override
      Storage client() {
        return mock;
      }
    };
  }

  private GCloudClient createClient(Storage mock, @Nullable StorageClass storage) {
    return createClient(
        mock,
        storage != null
            ? Collections.singletonMap("storage-class", storage.toString())
            : Collections.emptyMap());
  }
}
