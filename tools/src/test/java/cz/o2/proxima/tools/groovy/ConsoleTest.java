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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

/** Test {@link cz.o2.proxima.tools.groovy.Console} and various aspects of Closure/classloading. */
public class ConsoleTest {

  private final Config config = ConfigFactory.load("test-reference.conf").resolve();
  private final Repository repo = Repository.ofTest(config);

  @Test(timeout = 20000)
  public void testClosureLoading() throws Exception {
    ToolsClassLoader toolsLoader = new ToolsClassLoader();
    String script = "a = { it } \n";
    try (ClassLoaderFence fence = new ClassLoaderFence(toolsLoader);
        Console console = newConsole(script)) {
      console.run(toolsLoader);
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
    ToolsClassLoader toolsLoader = new ToolsClassLoader();
    String script = "a = { it } \n";
    EntityDescriptor gateway = repo.getEntity("gateway");
    AttributeDescriptor<byte[]> state = gateway.getAttribute("status");
    try (ClassLoaderFence fence = new ClassLoaderFence(toolsLoader);
        Console console = newConsole(script)) {

      assertNotNull(console.getRandomAccessReader("gateway"));
      assertNotNull(console.getBatchSnapshot(state));
      assertNotNull(console.getStream(state, Position.OLDEST, false, true));
    }
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

    ClassLoaderFence(ClassLoader newLoader) {
      original = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(newLoader);
    }

    @Override
    public void close() {
      Thread.currentThread().setContextClassLoader(original);
    }
  }
}
