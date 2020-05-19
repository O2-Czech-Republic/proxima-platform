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
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import cz.o2.proxima.direct.s3.S3Client.AmazonS3Factory;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class S3ClientTest {

  @Test
  public void testS3ClientFactory() {
    AmazonS3Factory factory = new AmazonS3Factory(cfg());
    AmazonS3 client = factory.build();
    assertNotNull(client);
  }

  @Test
  public void testPathParsing() {
    AmazonS3 mock = mock(AmazonS3.class);
    AtomicReference<PutObjectRequest> request = new AtomicReference<>();
    S3Client client =
        new S3Client(URI.create("s3://bucket/path"), cfg()) {
          @Override
          AmazonS3 client() {
            when(mock.putObject(any()))
                .thenAnswer(
                    invocationOnMock -> {
                      request.set(invocationOnMock.getArgumentAt(0, PutObjectRequest.class));
                      return null;
                    });
            return mock;
          }
        };
    assertEquals("bucket", client.getBucket());
    ByteArrayInputStream bais = new ByteArrayInputStream(new byte[] {1});
    client.putObject("object", bais);
    assertNotNull(request.get());
    assertEquals(client.getBucket(), request.get().getBucketName());
    assertEquals("object", request.get().getKey());
    assertEquals(bais, request.get().getInputStream());
  }

  private Map<String, Object> cfg() {
    return new HashMap<String, Object>() {
      {
        put("access-key", "access-key");
        put("secret-key", "secret-key");
        put("path-style-access", "true");
        put("endpoint", "http://endpoint321:123");
        put("signing-region", "signing-region");
        put("ssl-enable", "false");
      }
    };
  }
}
