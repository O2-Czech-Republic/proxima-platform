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
package cz.o2.proxima.utils.zookeeper;

import static org.junit.Assert.*;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.TimeProvider;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.util.TestUtils;
import cz.o2.proxima.utils.zookeeper.ZKGlobalWatermarkTracker.WatermarkWithUpdate;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test {@link ZKGlobalWatermarkTracker}. */
@Slf4j
public class ZKGlobalWatermarkTrackerTest {

  private static final File TMP =
      new File("/tmp/" + ZKGlobalWatermarkTracker.class.getSimpleName());
  private static final File WORK_DIR = new File(TMP, UUID.randomUUID().toString());
  private static final URI zkURI = URI.create("zk://localhost:12181/my-path");
  private static final ExecutorService executor = Executors.newCachedThreadPool();
  static Future<?> serverFuture;

  @BeforeClass
  public static void setupGlobal() throws IOException, ConfigException, InterruptedException {
    Preconditions.checkArgument(WORK_DIR.mkdirs());
    Runtime.getRuntime().addShutdownHook(new Thread(ZKGlobalWatermarkTrackerTest::deleteTemp));

    Properties props = new Properties();
    props.setProperty("clientPort", String.valueOf(zkURI.getPort()));
    props.setProperty("dataDir", WORK_DIR.getAbsolutePath());
    QuorumPeerConfig peerCfg = new QuorumPeerConfig();
    peerCfg.parseProperties(props);
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.readFrom(peerCfg);
    ZooKeeperServerMain zkServer = new ZooKeeperServerMain();
    serverFuture =
        executor.submit(
            () -> {
              try {
                zkServer.runFromConfig(serverConfig);
              } catch (Throwable err) {
                log.error("Failed to start server", err);
              }
            });
    TimeUnit.SECONDS.sleep(1);
  }

  private static void deleteTemp() {
    Path directory = Paths.get(TMP.toURI());
    ExceptionUtils.unchecked(
        () ->
            Files.walkFileTree(
                directory,
                new SimpleFileVisitor<Path>() {
                  @Override
                  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                      throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                  }

                  @Override
                  public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                      throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                  }
                }));
  }

  @AfterClass
  public static void tearDownGlobal() {
    serverFuture.cancel(true);
  }

  ZKGlobalWatermarkTracker tracker;

  @Before
  public void setUp() {
    tracker = newTracker();
  }

  private ZKGlobalWatermarkTracker newTracker() {
    return newTracker(cfg());
  }

  private ZKGlobalWatermarkTracker newTracker(Map<String, Object> cfg) {
    ZKGlobalWatermarkTracker tracker = new ZKGlobalWatermarkTracker();
    tracker.setup(cfg);
    return tracker;
  }

  private Map<String, Object> cfg() {
    return cfg("name-" + UUID.randomUUID().toString());
  }

  private Map<String, Object> cfg(String name) {
    return cfg(
        Pair.of(ZKGlobalWatermarkTracker.CFG_NAME, name),
        Pair.of(ZKGlobalWatermarkTracker.ZK_URI, zkURI.toASCIIString()));
  }

  @SafeVarargs
  private final Map<String, Object> cfg(Pair<String, String>... keyValues) {
    Builder<String, Object> builder = ImmutableMap.<String, Object>builder();
    Arrays.asList(keyValues).forEach(p -> builder.put(p.getFirst(), p.getSecond()));
    return builder.build();
  }

  @Test
  public void testPayloadParsing() {
    byte[] bytes = ZKGlobalWatermarkTracker.toPayload(1L, 2L);
    WatermarkWithUpdate payload = ZKGlobalWatermarkTracker.fromPayload(bytes);
    assertEquals(1L, (long) payload.getWatermark());
    assertEquals(2L, (long) payload.getTimestamp());
  }

  @Test(timeout = 10000)
  public void testGlobalWatermark() throws InterruptedException, ExecutionException {
    long now = Instant.now().toEpochMilli();
    assertEquals(Long.MIN_VALUE, tracker.getWatermark());
    tracker.initWatermarks(Collections.singletonMap("first", now));
    assertEquals(now, tracker.getWatermark());
    tracker.update("second", now + 1).get();
    assertEquals(now, tracker.getWatermark());
    tracker.update("first", now + 2).get();
    assertEquals(now + 1, tracker.getWatermark());
    tracker.finished("second").get();
    assertEquals(now + 2, tracker.getWatermark());
    tracker.finished("first").get();
    assertEquals(Long.MAX_VALUE, tracker.getWatermark());
  }

  @Test(timeout = 10000)
  public void testGlobalWatermarkWithStaleNode() throws InterruptedException, ExecutionException {
    tracker =
        newTracker(
            cfg(
                Pair.of(ZKGlobalWatermarkTracker.ZK_URI, zkURI.toASCIIString()),
                Pair.of(ZKGlobalWatermarkTracker.CFG_NAME, "name"),
                Pair.of(
                    ZKGlobalWatermarkTracker.CFG_TIME_PROVIDER, TestTimeProvider.class.getName()),
                Pair.of(ZKGlobalWatermarkTracker.CFG_MAX_ACCEPTABLE_UPDATE_AGE_MS, "1800000")));
    long now = Instant.now().toEpochMilli();
    ((TestTimeProvider) tracker.timeProvider).setCurrentTime(now);
    tracker.initWatermarks(Collections.singletonMap("first", now));
    ((TestTimeProvider) tracker.timeProvider).setCurrentTime(now + 3600000);
    long watermark = tracker.getGlobalWatermark("second", now + 1);
    assertEquals(now + 1, watermark);
  }

  @Test
  public void testGlobalWatermarkUpdate() throws ExecutionException, InterruptedException {
    long now = Instant.now().toEpochMilli();
    assertEquals(Long.MIN_VALUE, tracker.getWatermark());
    tracker.initWatermarks(Collections.singletonMap("first", now));
    assertEquals(now, tracker.getWatermark());
    tracker.update("second", now + 1).get();
    assertEquals(now, tracker.getWatermark());
    assertEquals(now + 1, tracker.getGlobalWatermark("first", now + 1));
  }

  @Test
  public void testTrackerSerializable() throws IOException, ClassNotFoundException {
    ZKGlobalWatermarkTracker clone =
        TestUtils.deserializeObject(TestUtils.serializeObject(tracker));
    assertNotNull(clone.pathToVersion);
  }

  @Test(timeout = 10000)
  public void testMultipleInstances() throws ExecutionException, InterruptedException {
    String name = tracker.getName();
    testMultipleInstancesWithTrackerFactory(() -> newTracker(cfg(name)), 10);
  }

  @Test(timeout = 60000)
  public void testConnectionLoss() throws ExecutionException, InterruptedException {
    String name = tracker.getName();
    testMultipleInstancesWithTrackerFactory(() -> createConnectionLossyTracker(name), 2);
  }

  @Test(timeout = 10000)
  public void testContainerNodeUnsupported() throws ExecutionException, InterruptedException {
    String name = tracker.getName();
    testMultipleInstancesWithTrackerFactory(
        () -> createUnimplementedContainerNodeTracker(name), 10);
  }

  @Test(timeout = 10000)
  public void testParentNodeDelete() throws ExecutionException, InterruptedException {
    String name = tracker.getName();
    testMultipleInstancesWithTrackerFactory(
        () ->
            createConnectionLossyTracker(
                name, keeper -> deleteRecursively(tracker.getParentNode(), keeper)),
        2);
  }

  private void deleteRecursively(String node, ZooKeeper keeper) {
    ExceptionUtils.unchecked(
        () -> {
          try {
            List<String> children = keeper.getChildren(node, false);
            children.forEach(
                ch -> ExceptionUtils.unchecked(() -> keeper.delete(node + "/" + ch, -1)));
            keeper.delete(node, -1);
          } catch (KeeperException ex) {
            // silently swallow this
          }
        });
  }

  private void testMultipleInstancesWithTrackerFactory(
      Factory<ZKGlobalWatermarkTracker> factory, int numInstances)
      throws ExecutionException, InterruptedException {

    List<ZKGlobalWatermarkTracker> trackers = Lists.newArrayList(tracker);
    while (trackers.size() < numInstances) {
      trackers.add(factory.apply());
    }
    long currentWatermark;
    long now = System.currentTimeMillis();
    long MIN_INSTANT = Long.MIN_VALUE;
    for (int i = 0; i < numInstances; i++) {
      trackers.get(i).initWatermarks(Collections.singletonMap("process" + i, MIN_INSTANT));
    }
    for (int i = 0; i < numInstances * 20; i++) {
      int id = i % numInstances;
      String process = "process" + id;
      ZKGlobalWatermarkTracker instance = trackers.get(id);
      instance.update(process, now + 1).get();
      currentWatermark = instance.getGlobalWatermark(process, now + 1);
      if (i > 2 * numInstances) {
        assertTrue(
            String.format(
                "Error in round %d, expected at least %d, got %d", i, now, currentWatermark),
            now <= currentWatermark);
      }
    }
    trackers.forEach(ZKGlobalWatermarkTracker::close);
  }

  @Test
  public void testGetNodeName() {
    assertEquals("node", ZKGlobalWatermarkTracker.getNodeName("parent/node"));
    assertEquals("node", ZKGlobalWatermarkTracker.getNodeName("node"));
  }

  private ZKGlobalWatermarkTracker createConnectionLossyTracker(String name) {
    return createConnectionLossyTracker(name, keeper -> {});
  }

  private ZKGlobalWatermarkTracker createConnectionLossyTracker(
      String name, Consumer<ZooKeeper> onClose) {
    Random stableRandom = new Random(0);
    ZKGlobalWatermarkTracker newTracker =
        new ZKGlobalWatermarkTracker() {
          @Override
          ZooKeeper createNewZooKeeper() {
            CountDownLatch latch = new CountDownLatch(1);
            ZooKeeper ret =
                ExceptionUtils.uncheckedFactory(
                    () ->
                        new LossyZooKeeper(
                            "localhost:12181", 60000, getWatcher(latch), stableRandom, onClose));
            ExceptionUtils.unchecked(latch::await);
            return ret;
          }
        };
    newTracker.setup(cfg(name));
    return newTracker;
  }

  private ZKGlobalWatermarkTracker createUnimplementedContainerNodeTracker(String name) {
    ZKGlobalWatermarkTracker newTracker =
        new ZKGlobalWatermarkTracker() {
          @Override
          ZooKeeper createNewZooKeeper() {
            CountDownLatch latch = new CountDownLatch(1);
            ZooKeeper ret =
                ExceptionUtils.uncheckedFactory(
                    () ->
                        new UnimplementedCreateContainerZooKeeper(
                            "localhost:12181", 60000, getWatcher(latch)));
            ExceptionUtils.unchecked(latch::await);
            return ret;
          }
        };
    newTracker.setup(cfg(name));
    return newTracker;
  }

  private static class LossyZooKeeper extends ZooKeeper {

    private final Random random;
    private final Consumer<ZooKeeper> onClose;

    public LossyZooKeeper(
        String connectString,
        int sessionTimeout,
        Watcher watcher,
        Random random,
        Consumer<ZooKeeper> onClose)
        throws IOException {

      super(connectString, sessionTimeout, watcher);
      this.random = random;
      this.onClose = onClose;
    }

    @Override
    public synchronized void close() throws InterruptedException {
      onClose.accept(this);
      super.close();
    }

    @Override
    public void setData(String path, byte[] data, int version, StatCallback cb, Object ctx) {
      if (shouldLooseConnection()) {
        ExceptionUtils.unchecked(this::close);
        cb.processResult(Code.CONNECTIONLOSS.intValue(), path, ctx, null);
      } else {
        super.setData(path, data, version, cb, ctx);
      }
    }

    @Override
    public void getData(String path, boolean watch, DataCallback cb, Object ctx) {
      if (shouldLooseConnection()) {
        cb.processResult(Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
      } else {
        super.getData(path, watch, cb, ctx);
      }
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
        throws KeeperException, InterruptedException {

      if (shouldLooseConnection()) {
        close();
        throw KeeperException.create(Code.CONNECTIONLOSS);
      }
      return super.create(path, data, acl, createMode);
    }

    private boolean shouldLooseConnection() {
      return random.nextDouble() < 0.3;
    }
  }

  private class UnimplementedCreateContainerZooKeeper extends ZooKeeper {

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
        throws KeeperException, InterruptedException {
      if (createMode == CreateMode.CONTAINER) {
        throw KeeperException.create(Code.UNIMPLEMENTED);
      }
      return super.create(path, data, acl, createMode);
    }

    public UnimplementedCreateContainerZooKeeper(
        String connectString, int sessionTimeout, Watcher watcher) throws IOException {
      super(connectString, sessionTimeout, watcher);
    }
  }

  public static class TestTimeProvider implements TimeProvider {
    @Setter @Getter long currentTime = Long.MIN_VALUE;
  }
}
