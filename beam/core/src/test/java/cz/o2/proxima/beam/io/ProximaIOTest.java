/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.io;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PDone;
import org.junit.Rule;
import org.junit.Test;

public class ProximaIOTest {

  private final Repository repository =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repository.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void writeTest() {
    PDone done =
        pipeline
            .apply(Create.of(Arrays.asList(write("key1"), write("key2"), write("key3"))))
            .apply(ProximaIO.write(repository.asFactory()));
    assertNotNull(pipeline.run());
    try (DirectDataOperator direct = repository.getOrCreateOperator(DirectDataOperator.class)) {
      RandomAccessReader reader = Optionals.get(direct.getRandomAccess(status));
      Optional<KeyValue<byte[]>> status1 = reader.get("key1", status);
      Optional<KeyValue<byte[]>> status2 = reader.get("key2", status);
      assertTrue(status2.isPresent());
      assertTrue(status1.isPresent());
    }
  }

  private StreamElement write(String key) {
    return StreamElement.upsert(
        gateway,
        status,
        UUID.randomUUID().toString(),
        key,
        status.getName(),
        System.currentTimeMillis(),
        new byte[] {1});
  }
}
