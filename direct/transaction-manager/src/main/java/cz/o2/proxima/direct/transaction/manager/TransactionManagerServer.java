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
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.commitlog.LogObservers.ForwardingObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.ExceptionUtils;
import cz.o2.proxima.util.Optionals;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
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

  private final Config conf;
  private final Repository repo;
  private final DirectDataOperator direct;
  private final List<ObserveHandle> runningObserves = new ArrayList<>();
  private final TransactionLogObserverFactory observerFactory;
  private AtomicBoolean closed = new AtomicBoolean();

  private TransactionManagerServer(Config conf, Repository repo) {
    this.conf = conf;
    this.repo = repo;
    this.direct = repo.getOrCreateOperator(DirectDataOperator.class);
    this.observerFactory = getObserverFactory(conf);
  }

  private TransactionLogObserverFactory getObserverFactory(Config conf) {
    return new TransactionLogObserverFactory.Default();
  }

  public synchronized void run() {
    List<AttributeFamilyDescriptor> transactionFamilies = getTransactionFamilies();
    Preconditions.checkArgument(
        !transactionFamilies.isEmpty(),
        "Repository contains no transactional families. No work, bailing out.");
    CountDownLatch awaitingLatch = new CountDownLatch(transactionFamilies.size());
    runInternal(transactionFamilies, awaitingLatch);
    if (ExceptionUtils.ignoringInterrupted(awaitingLatch::await)) {
      stop();
    } else {
      log.info("Started {}", getClass().getSimpleName());
    }
  }

  private List<AttributeFamilyDescriptor> getTransactionFamilies() {
    return repo.getAllFamilies(true)
        .filter(af -> af.getEntity().isSystemEntity())
        .filter(af -> af.getEntity().getName().equals("_transaction"))
        .collect(Collectors.toList());
  }

  private void runInternal(
      List<AttributeFamilyDescriptor> transactionFamilies, CountDownLatch awaitingLatch) {

    transactionFamilies.forEach(af -> startFamilyObserve(af, awaitingLatch));
  }

  private void startFamilyObserve(AttributeFamilyDescriptor family, CountDownLatch awaitingLatch) {
    ObserveHandle handle =
        Optionals.get(direct.getFamilyByName(family.getName()).getCommitLogReader())
            .observe(getObserverName(family), newTransformationLogObserver(awaitingLatch));
    runningObserves.add(handle);
  }

  private CommitLogObserver newTransformationLogObserver(CountDownLatch awaitingLatch) {
    return new ForwardingObserver(observerFactory.create(direct)) {
      boolean repartitioned = false;

      @Override
      public void onRepartition(OnRepartitionContext context) {
        if (!repartitioned) {
          awaitingLatch.countDown();
          repartitioned = true;
        }
        super.onRepartition(context);
      }
    };
  }

  protected String getObserverName(AttributeFamilyDescriptor family) {
    return "transaction-manager-" + family.getName();
  }

  public synchronized void stop() {
    if (!closed.getAndSet(true)) {
      log.info("{} shutting down.", getClass().getSimpleName());
      runningObserves.forEach(ObserveHandle::close);
      runningObserves.clear();
      direct.close();
    }
  }

  @VisibleForTesting
  boolean isStopped() {
    return closed.get();
  }
}
