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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.o2.proxima.storage.watermark.GlobalWatermarkTracker;
import cz.o2.proxima.util.ExceptionUtils;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/** A {@link GlobalWatermarkTracker} that stores global information in Apache Zookeeper. */
@Slf4j
public class ZKGlobalWatermarkTracker implements GlobalWatermarkTracker {

  private static final long serialVersionUID = 1L;
  private static final Instant MAX_WATERMARK = Instant.ofEpochMilli(Long.MAX_VALUE);

  static final String CFG_NAME = "name";
  static final String ZK_URI = "zk.url";
  static final String ZK_SESSION_TIMEOUT = "zk.timeout";

  private String trackerName;
  private String zkConnectString;
  private String parentNode;
  private int sessionTimeout;
  private transient volatile ZooKeeper client;

  @GuardedBy("this")
  private final Map<String, Long> partialWatermarks = new HashMap<>();

  @VisibleForTesting
  transient Map<String, Set<CompletableFuture<Void>>> updatedFutures = new ConcurrentHashMap<>();

  private final AtomicLong globalWatermark = new AtomicLong(Long.MIN_VALUE);
  private transient boolean parentCreated = false;
  private transient volatile int updateVersion = 0;

  @Override
  public String getName() {
    return trackerName;
  }

  @Override
  public void setup(Map<String, Object> cfg) {
    URI uri = getZkUri(cfg);
    zkConnectString = String.format("%s:%d", uri.getHost(), uri.getPort());
    sessionTimeout = getSessionTimeout(cfg);
    trackerName = getTrackerName(cfg);
    parentNode = normalize(Strings.isNullOrEmpty(uri.getPath()) ? "/" : uri.getPath());
  }

  // Normalize path to meet requirements needed by ZK
  // i.e. start with / and end with / to denote directory
  @VisibleForTesting
  static String normalize(String path) {
    if (path.equals("/")) {
      return path;
    }
    String res = path;
    if (!path.startsWith("/")) {
      res = "/" + path;
    }
    while (!res.isEmpty() && res.endsWith("/")) {
      res = res.substring(0, res.length() - 1);
    }
    return res + "/";
  }

  @Nonnull
  private String getTrackerName(Map<String, Object> cfg) {
    return Optional.ofNullable(cfg.get(CFG_NAME))
        .map(Object::toString)
        .orElseThrow(() -> new IllegalArgumentException("Missing " + CFG_NAME));
  }

  private int getSessionTimeout(Map<String, Object> cfg) {
    return Optional.ofNullable(cfg.get(ZK_SESSION_TIMEOUT))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(60000);
  }

  private URI getZkUri(Map<String, Object> cfg) {
    URI uri =
        Optional.ofNullable(cfg.get(ZK_URI))
            .map(Object::toString)
            .map(URI::create)
            .orElseThrow(() -> new IllegalArgumentException("Missing configuration " + ZK_URI));
    Preconditions.checkArgument(
        uri.getScheme().equalsIgnoreCase("zk"), "Unexpected scheme in %s, expected zk://", uri);
    return uri;
  }

  @Override
  public void initWatermarks(Map<String, Instant> initialWatermarks) {
    CountDownLatch latch = new CountDownLatch(initialWatermarks.size());
    initialWatermarks.forEach(
        (k, v) -> {
          ExceptionUtils.ignoringInterrupted(
              () -> persistPartialWatermark(k, v.toEpochMilli()).get());
          latch.countDown();
        });
    ExceptionUtils.ignoringInterrupted(latch::await);
  }

  @Override
  public CompletableFuture<Void> update(String processName, Instant currentWatermark) {
    if (currentWatermark.isBefore(MAX_WATERMARK)) {
      return persistPartialWatermark(processName, currentWatermark.toEpochMilli());
    }
    return deletePartialWatermark(processName);
  }

  @Override
  public Instant getGlobalWatermark() {
    if (!parentCreated) {
      ExceptionUtils.ignoringInterrupted(() -> createParentIfNotExists(getParentNode()));
    }
    return Instant.ofEpochMilli(globalWatermark.get());
  }

  private void createWatchForParentNode(String parent) throws InterruptedException {
    try {
      client().addWatch(parent, this::watchParentNode, AddWatchMode.PERSISTENT_RECURSIVE);
      for (String child : client().getChildren(parent, false)) {
        byte[] data = client().getData(parent + "/" + child, false, null);
        updatePartialWatermark(child, longFromBytes(data));
      }
    } catch (KeeperException ex) {
      if (ex.code() == Code.CONNECTIONLOSS || ex.code() == Code.SESSIONEXPIRED) {
        client = null;
        createWatchForParentNode(parent);
      } else {
        throw new RuntimeException(ex);
      }
    }
  }

  private void watchParentNode(WatchedEvent watchedEvent) {
    String path = watchedEvent.getPath();
    if (path != null && !path.equals(getParentNode())) {
      String process = getNodeName(watchedEvent.getPath());
      if (watchedEvent.getType() == EventType.NodeDeleted) {
        updatePartialWatermark(process, Long.MAX_VALUE);
      } else {
        client()
            .getData(
                path,
                false,
                (code, p, ctx, data, stat) -> {
                  long watermark = longFromBytes(data);
                  updatePartialWatermark(process, watermark);
                },
                null);
      }
    }
  }

  @VisibleForTesting
  static String getNodeName(@Nonnull String path) {
    int lastSlash = path.lastIndexOf("/");
    if (lastSlash < 0) {
      return path;
    }
    return path.substring(lastSlash + 1);
  }

  @Override
  public synchronized void close() {
    Optional.ofNullable(client)
        .ifPresent(
            c -> {
              // first nullify the client so that concurrent reads will not see closed client
              this.client = null;
              ExceptionUtils.ignoringInterrupted(c::close);
            });
    parentCreated = false;
  }

  private CompletableFuture<Void> persistPartialWatermark(String name, long watermark) {
    CompletableFuture<Void> updated = new CompletableFuture<>();
    addToUpdatedFutures(name, updated);
    CompletableFuture<Void> persisted = new CompletableFuture<>();
    try {
      String parent = getParentNode();
      createParentIfNotExists(parent);
      String node = parent + "/" + name;
      byte[] bytes = longAsBytes(watermark);
      client()
          .create(
              node,
              bytes,
              Ids.OPEN_ACL_UNSAFE,
              CreateMode.EPHEMERAL,
              (code, path, ctx, s) -> {
                if (code == Code.NODEEXISTS.intValue()) {
                  setNodeDataToFuture(path, bytes, persisted);
                } else if (code != Code.OK.intValue()) {
                  Throwable err =
                      new RuntimeException(
                          String.format("Failed to update watermark of %s: %s", name, code));
                  log.warn("Failed to update watermark", err);
                  persisted.completeExceptionally(err);
                } else {
                  persisted.complete(null);
                }
              },
              null);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      persisted.completeExceptionally(ex);
    }
    return CompletableFuture.allOf(persisted, updated);
  }

  private CompletableFuture<Void> deletePartialWatermark(String name) {
    CompletableFuture<Void> updated = new CompletableFuture<>();
    addToUpdatedFutures(name, updated);
    CompletableFuture<Void> persisted = new CompletableFuture<>();
    deleteNodeToFuture(name, persisted);
    return CompletableFuture.allOf(persisted, updated);
  }

  private void deleteNodeToFuture(String name, CompletableFuture<Void> res) {
    String parent = getParentNode();
    String node = parent + "/" + name;
    client()
        .delete(
            node,
            updateVersion,
            (code, path, ctx) -> {
              if (code == Code.CONNECTIONLOSS.intValue()
                  || code == Code.SESSIONEXPIRED.intValue()) {
                handleConnectionLoss(() -> deleteNodeToFuture(name, res));
              } else if (code == Code.BADVERSION.intValue()) {
                handleBadVersion(path, res, () -> deleteNodeToFuture(name, res));
              } else if (code != Code.OK.intValue() && code != Code.NONODE.intValue()) {
                Throwable err =
                    new RuntimeException(
                        String.format("Failed to delete watermark of %s: %s", name, code));
                log.warn("Failed to delete watermark", err);
                res.completeExceptionally(err);
              } else {
                res.complete(null);
              }
            },
            null);
  }

  private void addToUpdatedFutures(String name, CompletableFuture<Void> whenUpdated) {
    updatedFutures.compute(
        name,
        (k, v) -> {
          if (v == null) {
            v = new HashSet<>();
          }
          v.add(whenUpdated);
          return v;
        });
  }

  private void setNodeDataToFuture(String path, byte[] bytes, CompletableFuture<Void> res) {
    final int currentVersion = updateVersion;
    client()
        .setData(
            path,
            bytes,
            currentVersion,
            (code, p, ctx, stat) -> {
              if (code == Code.CONNECTIONLOSS.intValue()
                  || code == Code.SESSIONEXPIRED.intValue()) {
                handleConnectionLoss(() -> setNodeDataToFuture(path, bytes, res));
              } else if (code == Code.BADVERSION.intValue()) {
                handleBadVersion(path, res, () -> setNodeDataToFuture(path, bytes, res));
              } else if (code == Code.OK.intValue()) {
                updateVersion = stat.getVersion();
                res.complete(null);
              } else {
                Throwable err =
                    new RuntimeException(
                        String.format("Failed to update watermark of %s: %s", path, code));
                log.warn("Error updating watermark", err);
                res.completeExceptionally(err);
              }
            },
            null);
  }

  private CompletableFuture<Integer> getNodeVersion(String path) {
    CompletableFuture<Integer> res = new CompletableFuture<>();
    getNodeVersionToFuture(path, res);
    return res;
  }

  private void getNodeVersionToFuture(String path, CompletableFuture<Integer> res) {
    client()
        .getData(
            path,
            false,
            (code, p, ctx, bytes, stat) -> {
              if (code == Code.CONNECTIONLOSS.intValue()
                  || code == Code.SESSIONEXPIRED.intValue()) {
                handleConnectionLoss(() -> getNodeVersionToFuture(path, res));
              } else if (code != Code.OK.intValue()) {
                res.completeExceptionally(
                    new RuntimeException(
                        String.format("Error fetching version of %s: %d", path, code)));
              } else {
                res.complete(stat.getVersion());
              }
            },
            null);
  }

  private void handleConnectionLoss(Runnable retry) {
    client = null;
    retry.run();
  }

  private void handleBadVersion(String path, CompletableFuture<Void> res, Runnable onSuccess) {
    getNodeVersion(path)
        .whenComplete(
            (v, exc) -> {
              if (exc != null) {
                res.completeExceptionally(exc);
              } else {
                updateVersion = v;
                onSuccess.run();
              }
            });
  }

  private synchronized void createParentIfNotExists(String node) throws InterruptedException {
    try {
      if (!parentCreated) {
        client().create(node, new byte[] {}, Ids.OPEN_ACL_UNSAFE, CreateMode.CONTAINER);
        createWatchForParentNode(node);
        parentCreated = true;
      }
    } catch (KeeperException ex) {
      if (ex.code() == Code.CONNECTIONLOSS || ex.code() == Code.SESSIONEXPIRED) {
        client = null;
        createParentIfNotExists(node);
      } else if (ex.code() != Code.NODEEXISTS) {
        throw new RuntimeException(ex);
      } else {
        createWatchForParentNode(node);
      }
    }
  }

  private static byte[] longAsBytes(long watermark) {
    return String.valueOf(watermark).getBytes(StandardCharsets.UTF_8);
  }

  private static long longFromBytes(byte[] bytes) {
    return Long.parseLong(new String(bytes, StandardCharsets.UTF_8));
  }

  private String getParentNode() {
    return parentNode + this.trackerName;
  }

  private synchronized void updatePartialWatermark(String name, long watermark) {
    partialWatermarks.compute(
        name, (k, value) -> Math.max(value == null ? Long.MIN_VALUE : value, watermark));

    globalWatermark.set(
        partialWatermarks.values().stream().min(Long::compare).orElse(Long.MIN_VALUE));

    Optional.ofNullable(updatedFutures.remove(name))
        .ifPresent(l -> l.forEach(f -> f.complete(null)));
  }

  private ZooKeeper client() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = createNewZooKeeper();
        }
      }
    }
    return client;
  }

  @VisibleForTesting
  ZooKeeper createNewZooKeeper() {
    CountDownLatch connectLatch = new CountDownLatch(1);
    ZooKeeper zoo =
        ExceptionUtils.uncheckedFactory(
            () ->
                new ZooKeeper(
                    Objects.requireNonNull(zkConnectString),
                    sessionTimeout,
                    getWatcher(connectLatch)));
    ExceptionUtils.ignoringInterrupted(
        () -> {
          if (!connectLatch.await(10, TimeUnit.SECONDS)) {
            throw new RuntimeException("Timeout while connecting to ZK");
          }
        });
    return zoo;
  }

  @VisibleForTesting
  Watcher getWatcher(CountDownLatch connectLatch) {
    return event -> waitTillConnected(event, connectLatch::countDown);
  }

  private static void waitTillConnected(WatchedEvent event, Runnable whenConnected) {
    if (event.getState() == KeeperState.SyncConnected) {
      whenConnected.run();
    }
  }

  protected Object readResolve() {
    updatedFutures = new ConcurrentHashMap<>();
    return this;
  }
}
