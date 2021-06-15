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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogObservers.ForwardingObserver;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.transaction.ServerTransactionManager;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Repository;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
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
      LockSupport.park();
    } finally {
      server.stop();
    }
  }

  private final DirectDataOperator direct;
  private final ServerTransactionManager manager;
  private final TransactionLogObserverFactory observerFactory;
  private final AtomicBoolean closed = new AtomicBoolean();

  private TransactionManagerServer(Config conf, Repository repo) {
    this.direct = repo.getOrCreateOperator(DirectDataOperator.class);
    this.manager = direct.getServerTransactionManager();
    this.observerFactory = getObserverFactory(conf);
  }

  private TransactionLogObserverFactory getObserverFactory(Config conf) {
    return new TransactionLogObserverFactory.Default();
  }

  public void run() {
    manager.runObservations("transaction-manager", newTransformationLogObserver());
    log.info("Started {}", getClass().getSimpleName());
  }

  private CommitLogObserver newTransformationLogObserver() {
    return new ForwardingObserver(observerFactory.create(direct)) {
      @Override
      public boolean onError(Throwable error) {
        super.onError(error);
        log.error("Error processing transactions. Bailing out for safety.", error);
        System.exit(1);
        return false;
      }
    };
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
