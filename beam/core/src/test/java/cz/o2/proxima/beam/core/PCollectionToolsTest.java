/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core;

import static org.junit.Assert.assertNotNull;

import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.time.Instant;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

public class PCollectionToolsTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final EntityDescriptor event = repo.getEntity("event");
  private final Regular<byte[]> status = Regular.of(gateway, gateway.getAttribute("status"));
  private final Regular<byte[]> data = Regular.of(event, event.getAttribute("data"));
  private final Wildcard<byte[]> device = Wildcard.of(gateway, gateway.getAttribute("device.*"));

  @Test
  public void testReduceAsSnapshot() {
    Pipeline pipeline = Pipeline.create();
    Instant now = Instant.now();
    PCollection<StreamElement> input =
        pipeline.apply(
            Create.of(
                status.upsert("key", now, new byte[] {1}),
                data.upsert("key", now, new byte[] {2}),
                device.upsert("key", "suffix", now, new byte[] {3}),
                status.upsert("key2", now.plusMillis(1), new byte[] {4}),
                status.upsert("key", now.plusMillis(5), new byte[] {5})));

    PCollection<String> result =
        PCollectionTools.reduceAsSnapshot("reduce", input)
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        e ->
                            String.format(
                                "%s:%s:%s:%d",
                                e.getAttributeDescriptor().getEntity(),
                                e.getKey(),
                                e.getAttribute(),
                                e.getValue()[0])));
    PAssert.that(result)
        .containsInAnyOrder(
            "gateway:key:status:5",
            "event:key:data:2",
            "gateway:key:device.suffix:3",
            "gateway:key2:status:4");
    assertNotNull(pipeline.run());
  }
}
