/**
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
package cz.o2.proxima.direct.transform;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogObservers.TerminationStrategy;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transform.ElementWiseTransformation;
import cz.o2.proxima.util.Optionals;
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
    void doTransform(StreamElement ingest, OffsetCommitter committer) {

      AtomicInteger toConfirm = new AtomicInteger(0);
      try {
        ElementWiseTransformation.Collector<StreamElement> collector =
            elem -> {
              try {
                log.debug("Transformation {}: writing transformed element {}", name, elem);
                Optionals.get(direct().getWriter(elem.getAttributeDescriptor()))
                    .write(
                        elem,
                        (succ, exc) -> {
                          if (succ) {
                            onReplicated(elem);
                            if (toConfirm.decrementAndGet() == 0) {
                              committer.confirm();
                            }
                          } else {
                            toConfirm.set(-1);
                            committer.fail(exc);
                          }
                        });
              } catch (Exception ex) {
                toConfirm.set(-1);
                committer.fail(ex);
              }
            };
        if (toConfirm.addAndGet(transformation.apply(ingest, collector)) == 0) {
          committer.confirm();
        }
      } catch (Exception ex) {
        toConfirm.set(-1);
        committer.fail(ex);
      }
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
    void doTransform(StreamElement ingest, OffsetCommitter context) {
      log.debug("Transformation {}: processing input {}", name, ingest);
      transformation.transform(ingest, context::commit);
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
  public boolean onNext(StreamElement ingest, OnNextContext context) {
    log.debug(
        "Transformation {}: Received ingest {} at watermark {}",
        name,
        ingest,
        context.getWatermark());
    reportConsumerWatermark(name, context.getWatermark(), ingest.getStamp());
    if (!filter.apply(ingest)) {
      log.debug("Transformation {}: skipping transformation of {} by filter", name, ingest);
      context.confirm();
    } else {
      doTransform(ingest, context);
    }
    return true;
  }

  abstract void doTransform(StreamElement ingest, OffsetCommitter committer);

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
