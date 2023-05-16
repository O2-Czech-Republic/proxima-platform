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
package cz.o2.proxima.direct.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.core.util.ExceptionUtils;
import io.grpc.BindableService;
import io.grpc.Server;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import lombok.Getter;
import org.junit.Test;

public class IngestServerTest {

  private static class ExtendedIngestServer extends IngestServer {

    @Getter boolean serverRuns = false;
    @Getter boolean getServicesCalled = false;
    BlockingQueue<Server> createdServer = new ArrayBlockingQueue<>(1);

    protected ExtendedIngestServer(Config cfg) {
      super(cfg);
    }

    @Override
    protected void run() {
      serverRuns = true;
      getExecutor().execute(super::run);
      Server server = ExceptionUtils.uncheckedFactory(createdServer::take);
      assertNotNull(server);
      server.shutdown();
    }

    @Override
    protected Iterable<BindableService> getServices() {
      getServicesCalled = true;
      return super.getServices();
    }

    @Override
    protected Server createServer(int port, boolean debug) {
      return ExceptionUtils.uncheckedFactory(
          () -> {
            Server server = super.createServer(port, debug);
            createdServer.put(server);
            return server;
          });
    }
  }

  @Test(timeout = 20000)
  public void testIngestExtensible() {
    ExtendedIngestServer server =
        new ExtendedIngestServer(ConfigFactory.load("test-ingest-server.conf").resolve());
    IngestServer.runWithServerFactory(() -> server);
    assertTrue(server.isGetServicesCalled());
    assertTrue(server.isServerRuns());
  }

  @Test
  public void testCreateConfigFromArgs() {
    assertNotNull(IngestServer.getCfgFromArgs(new String[] {}));
  }
}
