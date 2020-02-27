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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.collect.Lists;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/** {@link FileSystem} implementation for gs://. */
@Internal
@Slf4j
public class GCloudFileSystem extends GCloudClient implements FileSystem {

  private final URI uri;
  private final NamingConvention namingConvention;

  GCloudFileSystem(GCloudStorageAccessor accessor) {
    super(accessor.getEntityDescriptor(), accessor.getUri(), accessor.getCfg());
    this.uri = accessor.getUri();
    this.namingConvention = accessor.getNamingConvention();
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public Stream<Path> list(long minTs, long maxTs) {
    return getBlobsInRange(minTs, maxTs).stream().map(blob -> BlobPath.of(this, blob));
  }

  @Override
  public Path newPath(long ts) {
    return BlobPath.of(this, createBlob(namingConvention.nameOf(ts)));
  }

  private List<Blob> getBlobsInRange(long startStamp, long endStamp) {
    List<Blob> ret = new ArrayList<>();
    Collection<String> prefixes = namingConvention.prefixesOf(startStamp, endStamp);
    prefixes.forEach(
        prefix -> {
          Page<Blob> p = client().list(this.bucket, BlobListOption.prefix(prefix));
          List<Blob> sorted = Lists.newArrayList(p.iterateAll());
          sorted.sort(Comparator.comparing(Blob::getName));
          for (Blob blob : sorted) {
            if (namingConvention.isInRange(blob.getName(), startStamp, endStamp)) {
              ret.add(blob);
            }
          }
        });
    log.debug("Parsed partitions {} for startStamp {}, endStamp {}", ret, startStamp, endStamp);
    return ret;
  }
}
