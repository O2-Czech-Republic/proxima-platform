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
package cz.o2.proxima.beam.core;

import static org.junit.Assert.assertNotNull;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

public class JoinProxyTransformTest {

  private final Config config =
      ConfigFactory.load("test-beam-proxy.conf")
          .resolve()
          .withFallback(ConfigFactory.load("test-reference.conf").resolve());
  private final Repository repo = Repository.ofTest(config);
  private final EntityDescriptor entity = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> armed = entity.getAttribute("armed");
  private final AttributeDescriptor<byte[]> status = entity.getAttribute("status");
  private final AttributeDescriptor<Integer> proxy = entity.getAttribute("armed-proxy");
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final BeamDataOperator beam = repo.getOrCreateOperator(BeamDataOperator.class);
  private final long now = System.currentTimeMillis();

  @Test
  public void testReadFromProxy() {
    Pipeline p = Pipeline.create();
    writeStatus("gw", now + 100);
    writeArmed("gw", now + 200);
    PCollection<StreamElement> input = beam.getBatchUpdates(p, proxy);
    PAssert.that(input.apply(toKeyAttribute()))
        .containsInAnyOrder("gateway.armed-proxy:gw.armed-proxy:1@" + (now + 200));
    assertNotNull(p.run());
  }

  @Test
  public void testReadFromProxyAsStream() {
    Pipeline p = Pipeline.create();
    writeStatus("gw", now + 100);
    writeArmed("gw", now + 200);
    PCollection<StreamElement> input =
        beam.getBatchUpdates(p, Long.MIN_VALUE, Long.MAX_VALUE, true, proxy);
    PAssert.that(input.apply(toKeyAttribute()))
        .containsInAnyOrder("gateway.armed-proxy:gw.armed-proxy:1@" + (now + 200));
    assertNotNull(p.run());
  }

  @Test
  public void testReadFromProxyFromStream() {
    Pipeline p = Pipeline.create();
    writeStatus("gw", now + 100);
    writeArmed("gw", now + 200);
    PCollection<StreamElement> input = beam.getStream(p, Position.OLDEST, true, true, proxy);
    PAssert.that(input.apply(toKeyAttribute()))
        .containsInAnyOrder("gateway.armed-proxy:gw.armed-proxy:1@" + (now + 200));
    assertNotNull(p.run());
  }

  @Test
  public void testReadFromProxyWithNoStatus() {
    Pipeline p = Pipeline.create();
    writeArmed("gw", now + 200);
    PCollection<StreamElement> input = beam.getBatchUpdates(p, proxy);
    PAssert.that(input.apply(toKeyAttribute())).empty();
    assertNotNull(p.run());
  }

  @Test
  public void testDeserializeWithContextProxy() {
    RepositoryFactory factory =
        RepositoryFactory.caching(RepositoryFactory.compressed(config), repo);
    RepositoryFactory.VersionedCaching.drop();
    Repository repo = factory.apply();
    assertNotNull(repo);
  }

  private static PTransform<PCollection<StreamElement>, PCollection<String>> toKeyAttribute() {
    return new PTransform<PCollection<StreamElement>, PCollection<String>>() {
      @Override
      public PCollection<String> expand(PCollection<StreamElement> input) {
        return input.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    e ->
                        String.format(
                            "%s.%s:%s.%s:%d@%d",
                            e.getEntityDescriptor().getName(),
                            e.getAttributeDescriptor().getName(),
                            e.getKey(),
                            e.getAttribute(),
                            (int) e.getParsed().get(),
                            e.getStamp())));
      }
    };
  }

  private void writeStatus(String gw, long stamp) {
    writeAttr(status, gw, stamp);
  }

  private void writeArmed(String gw, long stamp) {
    writeAttr(armed, gw, stamp);
  }

  private void writeAttr(AttributeDescriptor<?> attr, String gw, long stamp) {
    StreamElement upsert =
        StreamElement.upsert(
            entity, attr, UUID.randomUUID().toString(), gw, attr.getName(), stamp, new byte[] {1});
    writer(attr).write(upsert, ignoreStatus());
  }

  private CommitCallback ignoreStatus() {
    return (succ, exc) -> {};
  }

  private OnlineAttributeWriter writer(AttributeDescriptor<?> attr) {
    return direct
        .getWriter(armed)
        .orElseThrow(() -> new IllegalStateException("Missing writer for " + attr));
  }
}
