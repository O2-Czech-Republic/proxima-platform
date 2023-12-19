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
package cz.o2.proxima.beam.core.io;

import static org.junit.Assert.*;

import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.testing.Messages;
import cz.o2.proxima.beam.core.testing.Messages.Device;
import cz.o2.proxima.beam.core.testing.Messages.Status;
import cz.o2.proxima.beam.core.testing.Messages.User;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.core.repository.EntityAwareAttributeDescriptor.Wildcard;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

public class SchemaStreamElementCoderTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-schema-coder.conf").resolve());

  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final BeamDataOperator beam = repo.getOrCreateOperator(BeamDataOperator.class);

  private final SchemaStreamElementCoder coder = SchemaStreamElementCoder.of(repo);

  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final Regular<Status> gatewayStatus = Regular.of(gateway, gateway.getAttribute("status"));
  private final Wildcard<String> gatewayUser = Wildcard.of(gateway, gateway.getAttribute("user.*"));
  private final Wildcard<Device> device = Wildcard.of(gateway, gateway.getAttribute("device.*"));

  private final EntityDescriptor user = repo.getEntity("user");
  private final Regular<User> userDetails = Regular.of(user, user.getAttribute("details"));

  @Test
  public void testParsing() throws CoderException {
    StreamElement element =
        gatewayStatus.upsert(
            "key", System.currentTimeMillis(), Messages.Status.newBuilder().setAlive(true).build());
    byte[] encoded = CoderUtils.encodeToByteArray(coder, element);
    assertNotNull(encoded);
    StreamElement decoded = CoderUtils.decodeFromByteArray(coder, encoded);
    assertEquals(element, decoded);
  }

  @Test
  public void testParsingWildcard() throws CoderException {
    StreamElement element = gatewayUser.upsert("key", "user", System.currentTimeMillis(), "");
    byte[] encoded = CoderUtils.encodeToByteArray(coder, element);
    assertNotNull(encoded);
    StreamElement decoded = CoderUtils.decodeFromByteArray(coder, encoded);
    assertEquals(element, decoded);
  }

  @Test
  public void testQuery1() {
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> input =
        beam.getBatchSnapshot(p, gatewayStatus).setCoder(SchemaStreamElementCoder.of(repo));
    PCollection<Long> result =
        input
            .apply(SqlTransform.query("SELECT COUNT(*) FROM PCOLLECTION WHERE key = 'a'"))
            .apply(Convert.to(TypeDescriptors.longs()));
    PAssert.that(result).containsInAnyOrder(1L);

    long now = System.currentTimeMillis();
    write(gatewayStatus.upsert("a", now, Status.newBuilder().setAlive(true).build()));
    write(gatewayStatus.upsert("b", now, Status.newBuilder().setAlive(false).build()));

    p.run().waitUntilFinish();
  }

  @Test
  public void testQuery2() {
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> input =
        beam.getBatchUpdates(p, gatewayStatus).setCoder(SchemaStreamElementCoder.of(repo));
    PCollection<String> result =
        input
            .apply(
                SqlTransform.query(
                    "SELECT key FROM PCOLLECTION WHERE PCOLLECTION.gateway_status.alive = false"))
            .apply(Convert.to(TypeDescriptors.strings()));
    PAssert.that(result).containsInAnyOrder("b");

    long now = System.currentTimeMillis();
    write(gatewayStatus.upsert("a", now, Status.newBuilder().setAlive(true).build()));
    write(gatewayStatus.upsert("b", now, Status.newBuilder().setAlive(false).build()));

    p.run().waitUntilFinish();
  }

  @Test
  public void testQuery3() {
    long now = System.currentTimeMillis();
    write(gatewayUser.upsert("gw1", "u1", now, ""));
    write(gatewayUser.upsert("gw2", "u1", now, ""));
    write(gatewayUser.upsert("gw2", "u2", now, ""));

    Pipeline p = Pipeline.create();
    PCollection<StreamElement> gatewayUsers =
        beam.getBatchUpdates(p, gatewayUser).setCoder(SchemaStreamElementCoder.of(repo));
    PCollection<String> result =
        gatewayUsers
            .apply(SqlTransform.query("SELECT key, COUNT(*) c FROM PCOLLECTION GROUP BY key"))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(row -> String.format("%s:%d", row.getString(0), row.getInt64(1))));
    PAssert.that(result).containsInAnyOrder("gw1:1", "gw2:2");
    p.run().waitUntilFinish();
  }

  @Test
  public void testQuery4() {
    long now = System.currentTimeMillis();
    write(
        userDetails.upsert(
            "u1",
            now,
            User.newBuilder().addPhone("phone1").addPhone("phone2").setName("alice").build()));
    write(
        userDetails.upsert(
            "u2",
            now,
            User.newBuilder().addPhone("phone3").addPhone("phone4").setName("bob").build()));

    write(gatewayUser.upsert("gw1", "u1", now, ""));
    write(gatewayUser.upsert("gw2", "u1", now, ""));
    write(gatewayUser.upsert("gw2", "u2", now, ""));

    Pipeline p = Pipeline.create();
    PCollection<StreamElement> gatewayUsers =
        beam.getBatchUpdates(p, gatewayUser).setCoder(SchemaStreamElementCoder.of(repo));
    PCollection<StreamElement> users =
        beam.getBatchUpdates(p, userDetails).setCoder(SchemaStreamElementCoder.of(repo));

    PCollection<String> result =
        PCollectionTuple.of("gateway", gatewayUsers)
            .and("user", users)
            .apply(
                SqlTransform.query(
                    "SELECT u.user_details.name, COUNT(*) FROM `gateway` g "
                        + "JOIN `user` u ON u.key = g.attribute GROUP BY u.user_details.name"))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(row -> String.format("%s:%d", row.getString(0), row.getInt64(1))));
    PAssert.that(result).containsInAnyOrder("alice:2", "bob:1");

    p.run().waitUntilFinish();
  }

  private void write(StreamElement elem) {
    Optionals.get(direct.getWriter(elem.getAttributeDescriptor())).write(elem, (succ, exc) -> {});
  }
}
