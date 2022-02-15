/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.transaction;

import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.transform.DirectElementWiseTransform;
import cz.o2.proxima.repository.ConfigConstants;
import cz.o2.proxima.repository.EntityAwareAttributeDescriptor.Regular;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transaction.Commit;
import cz.o2.proxima.util.Optionals;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionCommitTransformation implements DirectElementWiseTransform {

  private final Map<String, OnlineAttributeWriter> writers = new HashMap<>();
  private RepositoryFactory repositoryFactory;
  private Regular<Commit> commitDesc;
  private transient DirectDataOperator direct;

  @Override
  public void setup(
      Repository repo, DirectDataOperator directDataOperator, Map<String, Object> cfg) {

    EntityDescriptor transaction = repo.getEntity(ConfigConstants.TRANSACTION_ENTITY);
    this.repositoryFactory = repo.asFactory();
    this.commitDesc =
        Regular.of(transaction, transaction.getAttribute(ConfigConstants.COMMIT_ATTRIBUTE));
  }

  @Override
  public void transform(StreamElement input, CommitCallback commitCallback) {
    if (input.getAttributeDescriptor().equals(commitDesc)) {
      Optional<Commit> commit = commitDesc.valueOf(input);
      if (commit.isPresent()) {
        handleCommit(commit.get(), commitCallback);
      } else {
        log.warn("Unparseable value in {}", input);
      }
    }
  }

  private void handleCommit(Commit commit, CommitCallback commitCallback) {
    log.debug("Received commit {}", commit);
    if (commit.getUpdates().isEmpty() && commit.getTransactionUpdates().isEmpty()) {
      log.warn("Received empty commit {}", commit);
      commitCallback.commit(true, null);
    } else if (commit.getUpdates().isEmpty()) {
      CommitCallback totalCallback =
          CommitCallback.afterNumCommits(commit.getTransactionUpdates().size(), commitCallback);
      commit
          .getTransactionUpdates()
          .forEach(
              update ->
                  getWriterForFamily(update.getTargetFamily())
                      .write(update.getUpdate(), totalCallback));
    } else {
      CommitCallback totalCallback =
          CommitCallback.afterNumCommits(commit.getUpdates().size(), commitCallback);
      commit
          .getUpdates()
          .forEach(
              update ->
                  nonTransactional(
                          Optionals.get(direct().getWriter(update.getAttributeDescriptor())))
                      .write(update, totalCallback));
    }
  }

  private DirectDataOperator direct() {
    if (direct == null) {
      direct = repositoryFactory.apply().getOrCreateOperator(DirectDataOperator.class);
    }
    return direct;
  }

  private OnlineAttributeWriter nonTransactional(OnlineAttributeWriter writer) {
    if (writer.isTransactional()) {
      return ((TransactionalOnlineAttributeWriter) writer).getDelegate();
    }
    return writer;
  }

  private OnlineAttributeWriter getWriterForFamily(String family) {
    return writers.computeIfAbsent(
        family, k -> Optionals.get(direct().getFamilyByName(family).getWriter()).online());
  }

  @Override
  public void close() {
    writers.values().forEach(OnlineAttributeWriter::close);
    writers.clear();
  }
}
