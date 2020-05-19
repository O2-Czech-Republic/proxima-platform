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
package cz.o2.proxima.direct.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.io.ByteStreams;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Value;
import org.junit.Before;
import org.junit.Test;

/** Test {@link S3FileSystem}. */
public class S3FileSystemTest {

  @Value
  private static class Blob {
    String name;
    byte[] contents;
    S3ObjectSummary summary;

    Blob(String name, byte[] contents) {
      this.name = name;
      this.contents = contents;
      summary = toSummary();
    }

    private final S3ObjectSummary toSummary() {
      S3ObjectSummary ret = mock(S3ObjectSummary.class);
      when(ret.getKey()).thenReturn(name);
      when(ret.getSize()).thenReturn((long) contents.length);
      return ret;
    }
  }

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private S3FileSystem fs;

  @Before
  public void setUp() {
    List<Blob> blobs = Collections.synchronizedList(new ArrayList<>());
    S3Accessor accessor = new S3Accessor(gateway, URI.create("s3://bucket/path"), cfg());
    fs =
        new S3FileSystem(accessor, direct.getContext()) {
          @Override
          AmazonS3 client() {
            AmazonS3 client = mock(AmazonS3.class);
            when(client.listObjects(any(), any())).thenAnswer(invocationOnMock -> asListing(blobs));
            when(client.putObject(any()))
                .thenAnswer(
                    invocationOnMock -> {
                      PutObjectRequest req =
                          invocationOnMock.getArgumentAt(0, PutObjectRequest.class);
                      InputStream input = req.getInputStream();
                      String name = req.getKey();
                      ByteArrayOutputStream baos = new ByteArrayOutputStream();
                      ByteStreams.copy(input, baos);
                      assertTrue(name.startsWith("path/"));
                      blobs.add(new Blob(name.substring(5), baos.toByteArray()));
                      return mock(PutObjectResult.class);
                    });
            return client;
          }
        };
  }

  static Map<String, Object> cfg() {
    return new HashMap<String, Object>() {
      {
        put("access-key", "access-key");
        put("secret-key", "secret-key");
      }
    };
  }

  private ObjectListing asListing(List<Blob> blobs) {
    ObjectListing listing = mock(ObjectListing.class);
    when(listing.getObjectSummaries())
        .thenReturn(blobs.stream().map(Blob::getSummary).collect(Collectors.toList()));
    return listing;
  }

  @Test
  public void testListPartitions() throws IOException {
    long now = 1500000000000L;
    write(now);
    write(now + 86400000L);
    List<Path> paths = fs.list(Long.MIN_VALUE, Long.MAX_VALUE).collect(Collectors.toList());
    assertEquals(2, paths.size());
  }

  private void write(long stamp) throws IOException {
    Path path = fs.newPath(stamp);
    try (OutputStream os = path.writer()) {
      os.write(new byte[] {1, 2, 3});
    }
  }
}
