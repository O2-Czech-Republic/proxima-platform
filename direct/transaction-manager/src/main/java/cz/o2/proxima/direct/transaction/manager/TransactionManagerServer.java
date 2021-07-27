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
package cz.o2.proxima.direct.transaction.manager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.ServerTransactionManager;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.TransactionMode;
import cz.o2.proxima.util.ExceptionUtils;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

/**
 * A transaction manager that takes a {@link Repository} with configured transactional entities and
 * attributes and runs (possibly distributed) transaction manager.
 *
 * <p>The manager's responsibility is to keep track of transaction state, process transactional
 * requests, notify requesters about the result using responses and keep track of the transactional
 * state.
 */
@Experimental("The manager is currently experimental and not intended for production usage.")
@Slf4j
public class TransactionManagerServer {

  public static TransactionManagerServer of(Config conf) {
    return new TransactionManagerServer(conf, Repository.of(conf));
  }

  public static TransactionManagerServer of(ConfigRepository repo) {
    return new TransactionManagerServer(repo.getConfig(), repo);
  }

  public static void main(String[] args) {
    final Config config;
    if (args.length > 0) {
      config = ConfigFactory.load(args[0]).resolve();
    } else {
      config = ConfigFactory.load().resolve();
    }
    TransactionManagerServer server = TransactionManagerServer.of(config);
    try {
      Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
      server.run();
      while (!Thread.currentThread().isInterrupted() && !server.isStopped()) {
        ExceptionUtils.ignoringInterrupted(() -> TimeUnit.SECONDS.sleep(10));
      }
    } finally {
      server.stop();
    }
  }

  private final DirectDataOperator direct;
  private final ServerTransactionManager manager;
  private final TransactionLogObserverFactory observerFactory;
  private final AtomicBoolean closed = new AtomicBoolean();

  @VisibleForTesting
  TransactionManagerServer(Config conf, Repository repo) {
    this.direct = repo.getOrCreateOperator(DirectDataOperator.class);
    this.manager = direct.getServerTransactionManager();
    this.observerFactory = getObserverFactory(conf);

    validateModeSupported(repo);
  }

  @VisibleForTesting
  void validateModeSupported(Repository repo) {
    Set<TransactionMode> supportedModes =
        Sets.newHashSet(TransactionMode.ALL, TransactionMode.NONE);
    repo.getAllEntities()
        .flatMap(e -> e.getAllAttributes().stream())
        .filter(a -> !supportedModes.contains(a.getTransactionMode()))
        .findAny()
        .ifPresent(
            a -> {
              throw new UnsupportedOperationException(
                  "Transaction mode of attribute " + a + " is not yet supported");
            });
  }

  private TransactionLogObserverFactory getObserverFactory(Config conf) {
    return new TransactionLogObserverFactory.WithOnErrorHandler(
        error -> {
          log.error("Error processing transactions. Bailing out for safety.", error);
          stop();
          System.exit(1);
        });
  }

  public void run() {
    TransactionLogObserver observer = newTransactionLogObserver();
    observer.run("transaction-manager");
    log.info("Started {}", getClass().getSimpleName());
  }

  private TransactionLogObserver newTransactionLogObserver() {
    return observerFactory.create(direct);
  }

  public void stop() {
    if (closed.compareAndSet(false, true)) {
      log.info("{} shutting down.", getClass().getSimpleName());
      manager.close();
      direct.close();
    }
  }

  @VisibleForTesting
  boolean isStopped() {
    return closed.get();
  }
}
