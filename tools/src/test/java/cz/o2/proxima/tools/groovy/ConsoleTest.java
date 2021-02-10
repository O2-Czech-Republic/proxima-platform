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
package cz.o2.proxima.tools.groovy;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.tools.io.ConsoleRandomReader;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

/** Test {@link cz.o2.proxima.tools.groovy.Console} and various aspects of Closure/classloading. */
public class ConsoleTest {

  private final Config config = ConfigFactory.load("test-reference.conf").resolve();
  private final Repository repo = Repository.ofTest(config);
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<byte[]> status = gateway.getAttribute("status");

  @Test(timeout = 20000)
  public void testClosureLoading() throws Exception {
    final ToolsClassLoader toolsLoader;
    String script = "a = { it } \n";
    try (ClassLoaderFence fence = new ClassLoaderFence();
        Console console = newConsole(script)) {
      console.run();
      toolsLoader = console.getToolsClassLoader();
    }
    List<String> definedClosures =
        toolsLoader
            .getDefinedClasses()
            .stream()
            .filter(c -> c.endsWith("_run_closure1"))
            .collect(Collectors.toList());
    assertEquals(1, definedClosures.size());
  }

  @Test
  public void testGetAccessors() {
    try (ClassLoaderFence fence = new ClassLoaderFence();
        Console console = newConsole()) {
      assertNotNull(console.getRandomAccessReader("gateway"));
      assertNotNull(console.getBatchSnapshot(status));
      assertNotNull(console.getStream(status, Position.OLDEST, false, true));
    }
  }

  @Test
  public void testPut() throws InterruptedException {
    try (ClassLoaderFence fence = new ClassLoaderFence();
        Console console = newConsole()) {
      assertTrue(console.getDirect().isPresent());
      console.put(gateway, status, "key", status.getName(), "\"\"");
      ConsoleRandomReader reader = console.getRandomAccessReader(gateway.getName());
      KeyValue<Object> kv = reader.get("key", status.getName());
      assertArrayEquals(new byte[] {}, kv.getValue());
      console.delete(gateway, status, kv.getKey(), kv.getAttribute(), kv.getStamp());
      assertNull(reader.get("key", status.getName()));
    }
  }

  private Console newConsole() {
    return newConsole("");
  }

  private Console newConsole(String script) {
    return new Console(config, repo, new String[] {}) {
      int i = 0;

      @Override
      int nextInputByte() {
        return i < script.length() ? script.charAt(i++) : -1;
      }

      @Override
      void initializeStreamProvider() {
        this.streamProvider =
            new StreamProvider() {
              @Override
              public void close() {}

              @SuppressWarnings("unchecked")
              @Override
              public Stream<StreamElement> getStream(
                  Position position,
                  boolean stopAtCurrent,
                  boolean eventTime,
                  TerminatePredicate terminateCheck,
                  AttributeDescriptor<?>... attrs) {

                return mock(Stream.class);
              }

              @SuppressWarnings("unchecked")
              @Override
              public WindowedStream<StreamElement> getBatchUpdates(
                  long startStamp,
                  long endStamp,
                  TerminatePredicate terminateCheck,
                  AttributeDescriptor<?>... attrs) {
                return mock(WindowedStream.class);
              }

              @SuppressWarnings("unchecked")
              @Override
              public WindowedStream<StreamElement> getBatchSnapshot(
                  long fromStamp,
                  long toStamp,
                  TerminatePredicate terminateCheck,
                  AttributeDescriptor<?>... attrs) {
                return mock(WindowedStream.class);
              }
            };
      }
    };
  }

  private static class ClassLoaderFence implements AutoCloseable {

    private final ClassLoader original;

    ClassLoaderFence() {
      original = Thread.currentThread().getContextClassLoader();
    }

    @Override
    public void close() {
      Thread.currentThread().setContextClassLoader(original);
    }
  }
}
