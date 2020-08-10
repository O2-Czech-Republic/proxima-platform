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
package cz.o2.proxima.beam.transforms;

import static org.junit.Assert.assertNotNull;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.Arrays;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

public class ExtractKeyToKvTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");

  @Test
  public void testToKv() {
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> elems = p.apply(Create.of(write("gw1"), write("gw2")));
    PCollection<KV<String, Long>> result =
        elems.apply(ExtractKeyToKv.fromStreamElements()).apply(Count.perKey());
    PAssert.that(result).containsInAnyOrder(Arrays.asList(KV.of("gw1", 1L), KV.of("gw2", 1L)));
    assertNotNull(p.run());
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
