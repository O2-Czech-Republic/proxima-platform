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
package cz.o2.proxima.direct.s3;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.storage.internal.AbstractDataAccessorFactory.Accept;
import java.net.URI;
import org.junit.Test;

public class S3StorageDescriptorTest {

  @Test
  public void testAcceptS3URI() {
    S3StorageDescriptor descriptor = new S3StorageDescriptor();
    assertEquals(Accept.ACCEPT, descriptor.accepts(URI.create("s3://bucket/path")));
    assertEquals(Accept.REJECT, descriptor.accepts(URI.create("s3a://bucket/path")));
  }
}
