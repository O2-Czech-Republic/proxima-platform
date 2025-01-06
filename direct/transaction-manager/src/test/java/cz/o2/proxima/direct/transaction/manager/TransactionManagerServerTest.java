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
package cz.o2.proxima.direct.transaction.manager;

import static org.junit.Assert.*;

import cz.o2.proxima.core.repository.ConfigRepository;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

/** Test suite for {@link TransactionManagerServer}. */
public class TransactionManagerServerTest {

  private final Config conf =
      ConfigFactory.defaultApplication()
          .withFallback(ConfigFactory.load("test-transactions.conf"))
          .resolve();
  private final ConfigRepository repo = ConfigRepository.Builder.of(conf).build();
  private final TransactionManagerServer server =
      new TransactionManagerServer(conf, repo) {
        @Override
        void validateModeSupported(Repository repo) {
          try {
            super.validateModeSupported(repo);
            fail("Should have thrown UnsupportedOperationException");
          } catch (UnsupportedOperationException ex) {
            // pass
          }
        }
      };

  @Test(timeout = 10000)
  public void testServerRunTearDown() {
    server.run();
    assertFalse(server.isStopped());
    server.stop(true);
    assertTrue(server.isStopped());
  }

  @Test(timeout = 10000)
  public void testServerAsyncTerminate() {
    AtomicBoolean terminated = new AtomicBoolean();
    server.asyncTerminate(() -> {}, () -> terminated.set(true));
    assertTrue(terminated.get());
    terminated.set(false);
    server.asyncTerminate(
        () -> ExceptionUtils.ignoringInterrupted(() -> TimeUnit.SECONDS.sleep(100)),
        () -> terminated.set(true));
    assertTrue(terminated.get());
    terminated.set(false);
    server.asyncTerminate(
        () -> {
          throw new RuntimeException("Error");
        },
        () -> terminated.set(true));
    assertTrue(terminated.get());
  }
}
