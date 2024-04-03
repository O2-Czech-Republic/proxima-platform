/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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

import static org.junit.Assert.assertTrue;

import cz.o2.proxima.beam.io.ProximaIO.WriteFn;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Optional;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProximaIOWriteFnTest {

  private final Repository repository =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repository.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");
  private final WriteFn writeFn = new WriteFn(repository.asFactory());
  private RandomAccessReader reader;

  @Before
  public void setup() {
    writeFn.setUp();
    reader = Optionals.get(writeFn.getDirect().getRandomAccess(status));
  }

  @After
  public void tearDown() {
    writeFn.tearDown();
  }

  @Test
  public void writeSuccessfullyTest() {
    long now = System.currentTimeMillis();
    writeFn.processElement(
        StreamElement.upsert(
            gateway,
            status,
            UUID.randomUUID().toString(),
            "key1",
            status.getName(),
            now,
            new byte[] {1}));
    Optional<KeyValue<byte[]>> keyValue = reader.get("key1", status, now);
    assertTrue(keyValue.isPresent());
  }
}
