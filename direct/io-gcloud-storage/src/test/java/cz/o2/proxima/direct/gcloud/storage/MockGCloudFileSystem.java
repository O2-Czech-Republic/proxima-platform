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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.Getter;

public class MockGCloudFileSystem implements FileSystem {

  private class MockPath extends GCloudBlobPath implements Path {

    @Getter final String name;
    byte[] contents;

    MockPath(String name) {
      this(name, new byte[] {});
    }

    MockPath(String name, byte[] contents) {
      this(name, contents, mockBlob(name, 0));
    }

    MockPath(String name, byte[] contents, Blob blob) {
      super(MockGCloudFileSystem.this, blob);
      this.name = name;
      this.contents = contents;
    }

    @Override
    public InputStream reader() {
      return new ByteArrayInputStream(contents);
    }

    @Override
    public OutputStream writer() {
      return new ByteArrayOutputStream() {
        @Override
        public void close() throws IOException {
          super.close();
          contents = this.toByteArray();
        }
      };
    }

    @Override
    public FileSystem getFileSystem() {
      return MockGCloudFileSystem.this;
    }

    @Override
    public void delete() {
      paths.remove(name);
    }
  }

  private static Blob mockBlob(String name, long size) {
    Blob mock = mock(Blob.class);
    when(mock.getName()).thenReturn(name);
    when(mock.getSize()).thenReturn(size);
    return mock;
  }

  private final NamingConvention namingConvention;
  private final Map<String, MockPath> paths = new LinkedHashMap<>();

  MockGCloudFileSystem(NamingConvention namingConvention) {
    this.namingConvention = namingConvention;
  }

  @Override
  public URI getUri() {
    return URI.create("mock-gs://bucket/path");
  }

  @Override
  public Stream<Path> list(long minTs, long maxTs) {
    return paths
        .values()
        .stream()
        .filter(p -> namingConvention.isInRange(p.getName(), minTs, maxTs))
        .map(Function.identity());
  }

  @Override
  public Path newPath(long ts) {
    String path = namingConvention.nameOf(ts);
    return paths.compute(path, (k, v) -> new MockPath(k));
  }
}
