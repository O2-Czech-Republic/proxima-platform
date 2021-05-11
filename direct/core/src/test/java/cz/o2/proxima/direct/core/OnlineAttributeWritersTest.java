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
package cz.o2.proxima.direct.core;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.UUID;
import org.junit.Test;

public class OnlineAttributeWritersTest {

  private final Repository repo =
      Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");

  @Test
  public void testSynchronizedWriter() {
    OnlineAttributeWriter mock = mock(OnlineAttributeWriter.class);
    OnlineAttributeWriter writer = OnlineAttributeWriters.synchronizedWriter(mock);
    StreamElement element =
        StreamElement.upsert(
            gateway,
            status,
            UUID.randomUUID().toString(),
            "gw",
            status.getName(),
            System.currentTimeMillis(),
            new byte[] {});
    writer.write(element, (succ, exc) -> {});
    verify(mock).write(eq(element), any());
    writer.asFactory();
    verify(mock).asFactory();
    writer.getUri();
    verify(mock).getUri();
    writer.rollback();
    verify(mock).rollback();
    writer.close();
    verify(mock).close();
  }
}
