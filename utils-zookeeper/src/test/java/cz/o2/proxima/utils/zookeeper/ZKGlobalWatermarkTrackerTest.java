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
import com.google.common.collect.Lists;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.storage.watermark.GlobalWatermarkTracker;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.TestUtils;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AddWatchMode;
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
  private static final URI zkURI = URI.create("zk://localhost:12181");
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
    return ImmutableMap.<String, Object>builder()
        .put(ZKGlobalWatermarkTracker.CFG_NAME, name)
        .put(ZKGlobalWatermarkTracker.ZK_URI, zkURI.toASCIIString())
        .build();
  }

  @Test(timeout = 10000)
  public void testGlobalWatermark() throws InterruptedException, ExecutionException {
    Instant now = Instant.now();
    assertEquals(Instant.ofEpochMilli(Long.MIN_VALUE), tracker.getGlobalWatermark());
    tracker.initWatermarks(Collections.singletonMap("first", now));
    assertEquals(now, tracker.getGlobalWatermark());
    tracker.update("second", now.plusMillis(1)).get();
    assertEquals(now, tracker.getGlobalWatermark());
    tracker.update("first", now.plusMillis(2)).get();
    assertEquals(now.plusMillis(1), tracker.getGlobalWatermark());
    tracker.finished("second").get();
    assertEquals(now.plusMillis(2), tracker.getGlobalWatermark());
    tracker.finished("first").get();
    assertEquals(Instant.ofEpochMilli(Long.MAX_VALUE), tracker.getGlobalWatermark());
  }

  @Test
  public void testTrackerSerializable() throws IOException, ClassNotFoundException {
    ZKGlobalWatermarkTracker clone =
        TestUtils.deserializeObject(TestUtils.serializeObject(tracker));
    assertNotNull(clone.updatedFutures);
  }

  @Test(timeout = 60000)
  public void testMultipleInstances() throws ExecutionException, InterruptedException {
    String name = tracker.getName();
    testMultipleInstancesWithTrackerFactory(() -> newTracker(cfg(name)), 10);
  }

  @Test(timeout = 60000)
  public void testConnectionLoss() throws ExecutionException, InterruptedException {
    try {
      String name = tracker.getName();
      testMultipleInstancesWithTrackerFactory(() -> createConnectionLossyTracker(name), 2);
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw ex;
    }
  }

  private void testMultipleInstancesWithTrackerFactory(
      Factory<ZKGlobalWatermarkTracker> factory, int numInstances)
      throws ExecutionException, InterruptedException {
    String name = tracker.getName();
    List<ZKGlobalWatermarkTracker> trackers = Lists.newArrayList(tracker);
    while (trackers.size() < numInstances) {
      trackers.add(factory.apply());
    }
    long currentWatermark = Long.MIN_VALUE;
    long now = System.currentTimeMillis();
    tracker.initWatermarks(
        IntStream.range(0, numInstances)
            .mapToObj(i -> "process" + i)
            .collect(
                Collectors.toMap(
                    Function.identity(), tmp -> Instant.ofEpochMilli(Long.MIN_VALUE))));
    for (int i = 0; i < numInstances * 20; i++) {
      int id = i % numInstances;
      GlobalWatermarkTracker instance = trackers.get(id);
      long instanceWatermark = instance.getWatermark();
      assertTrue(
          String.format(
              "Expected watermark at least %d, got %d", currentWatermark, instanceWatermark),
          instanceWatermark >= currentWatermark);
      instance.update("process" + id, Instant.ofEpochMilli(now + i)).get();
      currentWatermark = instance.getWatermark();
      if (i < numInstances - 1) {
        assertEquals(String.format("Error in round %d", i), Long.MIN_VALUE, currentWatermark);
      } else {
        assertTrue(
            String.format(
                "Error in round %d, expected at least %d, got %d", i, now, currentWatermark),
            now <= currentWatermark);
      }
    }
    trackers.forEach(ZKGlobalWatermarkTracker::close);
  }

  @Test
  public void testPathNormalization() {
    assertEquals("/path/", ZKGlobalWatermarkTracker.normalize("path"));
    assertEquals("/path/", ZKGlobalWatermarkTracker.normalize("/path//"));
    assertEquals("/a/b/", ZKGlobalWatermarkTracker.normalize("/a/b/"));
    assertEquals("/", ZKGlobalWatermarkTracker.normalize("/"));
    assertEquals("/", ZKGlobalWatermarkTracker.normalize(""));
  }

  @Test
  public void testGetNodeName() {
    assertEquals("node", ZKGlobalWatermarkTracker.getNodeName("parent/node"));
    assertEquals("node", ZKGlobalWatermarkTracker.getNodeName("node"));
  }

  private ZKGlobalWatermarkTracker createConnectionLossyTracker(String name) {
    Random stableRandom = new Random(0);
    ZKGlobalWatermarkTracker newTracker =
        new ZKGlobalWatermarkTracker() {
          @Override
          ZooKeeper createNewZooKeeper() {
            CountDownLatch latch = new CountDownLatch(1);
            LossyZooKeeper ret =
                ExceptionUtils.uncheckedFactory(
                    () ->
                        new LossyZooKeeper(
                            "localhost:12181", 60000, getWatcher(latch), stableRandom));
            ExceptionUtils.unchecked(latch::await);
            return ret;
          }
        };
    newTracker.setup(cfg(name));
    return newTracker;
  }

  private static class LossyZooKeeper extends ZooKeeper {

    private final Random random;

    public LossyZooKeeper(String connectString, int sessionTimeout, Watcher watcher, Random random)
        throws IOException {

      super(connectString, sessionTimeout, watcher);
      this.random = random;
    }

    @Override
    public void setData(String path, byte[] data, int version, StatCallback cb, Object ctx) {
      if (random.nextBoolean()) {
        ExceptionUtils.unchecked(this::close);
        cb.processResult(Code.CONNECTIONLOSS.intValue(), path, ctx, null);
      } else {
        super.setData(path, data, version, cb, ctx);
      }
    }

    @Override
    public void getData(String path, boolean watch, DataCallback cb, Object ctx) {
      if (random.nextBoolean()) {
        ExceptionUtils.unchecked(this::close);
        cb.processResult(Code.CONNECTIONLOSS.intValue(), path, ctx, null, null);
      } else {
        super.getData(path, watch, cb, ctx);
      }
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
        throws KeeperException, InterruptedException {

      if (random.nextBoolean()) {
        close();
        throw KeeperException.create(Code.CONNECTIONLOSS);
      }
      return super.create(path, data, acl, createMode);
    }

    @Override
    public void addWatch(String basePath, Watcher watcher, AddWatchMode mode)
        throws KeeperException, InterruptedException {
      if (random.nextBoolean()) {
        close();
        throw KeeperException.create(Code.CONNECTIONLOSS);
      }
      super.addWatch(basePath, watcher, mode);
    }
  }
}
