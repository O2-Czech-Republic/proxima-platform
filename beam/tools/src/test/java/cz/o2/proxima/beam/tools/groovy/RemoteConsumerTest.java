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
package cz.o2.proxima.beam.tools.groovy;

import static org.junit.Assert.*;

import cz.o2.proxima.core.functional.Consumer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.junit.Test;

/** Test {@link RemoteConsumer}. */
public class RemoteConsumerTest {

  private final Random random = new Random();

  @Test
  public void testConsumeOk() {
    testConsumeOkWithPort(-1);
  }

  @Test(expected = RuntimeException.class)
  public void testConsumeException() {
    try (RemoteConsumer<String> remoteConsumer =
        RemoteConsumer.create(this, "localhost", -1, throwingConsumer(), StringUtf8Coder.of())) {
      remoteConsumer.accept("test");
    }
  }

  @Test
  public void testBindOnSpecificPort() {
    for (int i = 0; i < 10; i++) {
      int port = random.nextInt(65535 - 1024) + 1024;
      try {
        testConsumeOkWithPort(port);
        return;
      } catch (Exception ex) {
        if (!ex.getMessage().equals("Retries exhausted trying to start server")) {
          throw ex;
        }
      }
    }
    fail("Retries exhausted trying to run server on random port");
  }

  @Test
  public void testBindError() {
    for (int i = 0; i < 10; i++) {
      int port = random.nextInt(65535 - 1024) + 1024;
      List<String> list = new ArrayList<>();
      try {
        try (RemoteConsumer<String> remoteConsumer =
            RemoteConsumer.create(this, "localhost", port, list::add, StringUtf8Coder.of())) {
          try {
            RemoteConsumer<String> test =
                new RemoteConsumer<>("localhost", port, list::add, StringUtf8Coder.of());
            test.start();
            fail("Should have thrown exception");
          } catch (Exception ex) {
            assertTrue(RemoteConsumer.isBindException(ex));
            break;
          }
        }
      } catch (Exception ex) {
        // nop
      }
    }
    assertFalse(RemoteConsumer.isBindException(new RuntimeException()));
    assertFalse(RemoteConsumer.isBindException(new IOException()));
    assertFalse(RemoteConsumer.isBindException(new IOException("foo")));
  }

  @Test(expected = RuntimeException.class)
  public void testRebindOnSamePort() {
    int port = 43764;
    try (RemoteConsumer<String> consumer1 =
        RemoteConsumer.create(this, "localhost", port, tmp -> {}, StringUtf8Coder.of())) {
      RemoteConsumer.create(this, "localhost", port, tmp -> {}, StringUtf8Coder.of());
    }
  }

  private void testConsumeOkWithPort(int port) {
    List<String> consumed = new ArrayList<>();
    try (RemoteConsumer<String> remoteConsumer =
        RemoteConsumer.create(this, "localhost", port, consumed::add, StringUtf8Coder.of())) {
      remoteConsumer.accept("test");
    }
    assertEquals(1, consumed.size());
    assertEquals("test", consumed.get(0));
  }

  private <T> Consumer<T> throwingConsumer() {
    return tmp -> {
      throw new RuntimeException("Fail");
    };
  }
}
