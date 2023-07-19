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
package cz.o2.proxima.direct.io.pubsub;

import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.internal.AbstractDataAccessorFactory.Accept;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import org.junit.Test;
import org.mockito.Mockito;

/** Test suite for {@link PubSubStorage}. */
public class PubSubStorageTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-pubsub.conf").resolve());
  private final PubSubStorage storage = new PubSubStorage();

  @Test
  public void testDefaultValuesOverrides() {
    storage.setup(repo);
    assertEquals(3600000, storage.getDefaultMaxAckDeadlineMs());
    assertFalse(storage.isDefaultSubscriptionAutoCreate());
    assertEquals(10, storage.getDefaultSubscriptionAckDeadlineSeconds());
    assertNull(storage.getDefaultWatermarkEstimateDuration());
    assertEquals(200, storage.getDefaultAllowedTimestampSkew());
  }

  @Test
  public void testAccept() {
    assertEquals(Accept.ACCEPT, storage.accepts(URI.create("gps://project/topic")));
    assertEquals(Accept.REJECT, storage.accepts(URI.create("file:///dev/null")));
  }

  @Test
  public void testCreateAccessor() {
    final AttributeFamilyDescriptor family = Mockito.mock(AttributeFamilyDescriptor.class);
    Mockito.when(family.getStorageUri()).thenReturn(URI.create("gps://project/topic"));
    assertNotNull(
        storage.createAccessor(repo.getOrCreateOperator(DirectDataOperator.class), family));
  }
}