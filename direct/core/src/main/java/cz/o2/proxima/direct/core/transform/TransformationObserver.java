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
package cz.o2.proxima.direct.core.transform;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.repository.RepositoryFactory;
import cz.o2.proxima.core.storage.StorageFilter;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.transform.ElementWiseTransformation;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObservers.TerminationStrategy;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedRuntimeException;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/** Observer of source data performing transformation to another entity/attribute. */
@Slf4j
@Internal
public abstract class TransformationObserver implements CommitLogObserver {

  public static class NonContextual extends TransformationObserver {

    private final ElementWiseTransformation transformation;

    public NonContextual(
        DirectDataOperator direct,
        String name,
        ElementWiseTransformation transformation,
        boolean supportTransactions,
        StorageFilter filter) {

      super(direct, name, supportTransactions, filter);
      this.transformation = transformation;
    }

    @Override
    void doTransform(StreamElement element, OffsetCommitter committer) {

      AtomicInteger toConfirm = new AtomicInteger(0);
      try {
        ElementWiseTransformation.Collector<StreamElement> collector =
            elem -> collectWrittenElement(committer, toConfirm, elem);

        if (toConfirm.addAndGet(transformation.apply(element, collector)) == 0) {
          committer.confirm();
        }
      } catch (Exception ex) {
        failCommit(committer, toConfirm, ex);
      }
    }

    private void collectWrittenElement(
        OffsetCommitter committer, AtomicInteger toConfirm, StreamElement elem) {

      collectWrittenElement(committer, toConfirm, elem, 0);
    }

    private void collectWrittenElement(
        OffsetCommitter committer, AtomicInteger toConfirm, StreamElement elem, int retryNum) {

      try {
        log.debug("Transformation {}: writing transformed element {}", name, elem);
        getOnlineWriterFor(elem)
            .write(
                elem,
                (succ, exc) -> {
                  if (succ) {
                    onReplicated(elem);
                    if (toConfirm.decrementAndGet() == 0) {
                      committer.confirm();
                    }
                  } else if (retryNum < 5 && exc instanceof TransactionRejectedException) {
                    TransactionRejectedException ex = (TransactionRejectedException) exc;
                    log.info(
                        "Failed transaction {} flags {} for element {}, going to retry.",
                        ex.getTransactionId(),
                        ex.getResponseFlags(),
                        elem);
                    collectWrittenElement(committer, toConfirm, elem, retryNum + 1);
                  } else {
                    failCommit(committer, toConfirm, exc);
                  }
                });
      } catch (Exception ex) {
        failCommit(committer, toConfirm, ex);
      }
    }

    private void failCommit(OffsetCommitter committer, AtomicInteger toConfirm, Throwable exc) {
      toConfirm.set(-1);
      committer.fail(exc);
    }

    @VisibleForTesting
    OnlineAttributeWriter getOnlineWriterFor(StreamElement elem) {
      return Optionals.get(direct().getWriter(elem.getAttributeDescriptor()));
    }
  }

  public static class Contextual extends TransformationObserver {

    private final DirectElementWiseTransform transformation;

    public Contextual(
        DirectDataOperator direct,
        String name,
        DirectElementWiseTransform transformation,
        boolean supportTransactions,
        StorageFilter filter) {

      super(direct, name, supportTransactions, filter);
      this.transformation = transformation;
    }

    @Override
    void doTransform(StreamElement element, OffsetCommitter context) {
      log.debug("Transformation {}: processing input {}", name, element);
      CommitCallback commitCallback =
          (succ, exc) -> {
            onReplicated(element);
            context.commit(succ, exc);
          };
      for (int i = 0; ; i++) {
        try {
          transformation.transform(element, commitCallback);
          break;
        } catch (TransactionRejectedRuntimeException ex) {
          log.info(
              "Caught {}. Retries so far {}. {}",
              ex.getClass().getSimpleName(),
              i,
              i < 2 ? "Retrying." : "Rethrowing.");
          if (i == 2) {
            throw ex;
          }
        }
      }
    }
  }

  final String name;
  final RepositoryFactory repoFactory;
  transient DirectDataOperator direct;
  final boolean supportTransactions;
  final StorageFilter filter;

  TransformationObserver(
      DirectDataOperator direct, String name, boolean supportTransactions, StorageFilter filter) {

    this.name = name;
    this.repoFactory = direct.getRepository().asFactory();
    this.supportTransactions = supportTransactions;
    this.filter = filter;
  }

  @Override
  public boolean onError(Throwable error) {
    return true;
  }

  public TerminationStrategy onFatalError(Throwable error) {
    String msg = String.format("Failed to transform using %s. Bailing out.", name);
    log.error(msg, error);
    die(msg);
    return TerminationStrategy.RETHROW;
  }

  @Override
  public void onIdle(OnIdleContext context) {
    reportConsumerWatermark(name, context.getWatermark(), -1);
  }

  @Override
  public boolean onNext(StreamElement element, OnNextContext context) {
    log.debug(
        "Transformation {}: Received element {} at watermark {}",
        name,
        element,
        context.getWatermark());
    reportConsumerWatermark(name, context.getWatermark(), element.getStamp());
    if (!filter.apply(element)) {
      log.debug("Transformation {}: skipping transformation of {} by filter", name, element);
      context.confirm();
    } else {
      doTransform(element, context);
    }
    return true;
  }

  abstract void doTransform(StreamElement element, OffsetCommitter committer);

  DirectDataOperator direct() {
    if (direct == null) {
      direct = repoFactory.apply().getOrCreateOperator(DirectDataOperator.class);
    }
    return direct;
  }

  protected void reportConsumerWatermark(String name, long watermark, long elementStamp) {
    // nop
  }

  protected void die(String msg) {
    throw new RuntimeException(msg);
  }

  protected void onReplicated(StreamElement element) {
    // nop
  }
}
