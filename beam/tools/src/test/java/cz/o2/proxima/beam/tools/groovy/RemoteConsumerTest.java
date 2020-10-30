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
package cz.o2.proxima.beam.tools.groovy;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.functional.Consumer;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.junit.Test;

/** Test {@link RemoteConsumer}. */
public class RemoteConsumerTest {

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
    testConsumeOkWithPort(43764);
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
