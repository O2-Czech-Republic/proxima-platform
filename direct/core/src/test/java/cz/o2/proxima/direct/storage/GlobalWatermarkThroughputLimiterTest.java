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
package cz.o2.proxima.direct.storage;

import static org.junit.Assert.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.util.Optional;
import org.junit.Test;

/** Test parsing of {@link cz.o2.proxima.storage.ThroughputLimiter} from config. */
public class GlobalWatermarkThroughputLimiterTest {

  private final Config config =
      ConfigFactory.load("test-limiter.conf")
          .withFallback(ConfigFactory.load("test-reference.conf"))
          .resolve();
  private final Repository repo = Repository.ofTest(config);
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor event = repo.getEntity("event");
  private final AttributeDescriptor<byte[]> data = event.getAttribute("data");
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> armed = gateway.getAttribute("armed");

  @Test
  public void testEventDataCommitLogLimited() {
    Optional<CommitLogReader> reader = direct.getCommitLogReader(data);
    assertTrue(reader.isPresent());
    assertEquals("LimitedCommitLogReader", reader.get().getClass().getSimpleName());
    reader = direct.getCommitLogReader(armed);
    assertTrue(reader.isPresent());
    assertNotEquals("LimitedCommitLogReader", reader.get().getClass().getSimpleName());
  }
}
