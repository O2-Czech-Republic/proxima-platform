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
package cz.o2.proxima.server;

import static cz.o2.proxima.server.IngestServer.ingestRequest;

import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObservers.TerminationStrategy;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.server.metrics.Metrics;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.transform.ElementWiseTransformation;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/** Observer of source data performing transformation to another entity/attribute. */
@Slf4j
public class TransformationObserver implements LogObserver {

  private final RepositoryFactory repoFactory;
  private final ElementWiseTransformation transformation;
  private final boolean supportTransactions;
  private final StorageFilter filter;
  private final String name;

  private transient DirectDataOperator direct;

  TransformationObserver(
      DirectDataOperator direct,
      String name,
      ElementWiseTransformation transformation,
      boolean supportTransactions,
      StorageFilter filter) {

    this.repoFactory = direct.getRepository().asFactory();
    this.name = name;
    this.transformation = transformation;
    this.supportTransactions = supportTransactions;
    this.filter = filter;
  }

  @Override
  public boolean onError(Throwable error) {
    return true;
  }

  public TerminationStrategy onFatalError(Throwable error) {
    Utils.die(String.format("Failed to transform using %s. Bailing out.", transformation));
    return TerminationStrategy.RETHROW;
  }

  @Override
  public boolean onNext(StreamElement ingest, OnNextContext context) {
    Metrics.reportConsumerWatermark(name, context.getWatermark(), ingest.getStamp());
    if (!filter.apply(ingest)) {
      log.debug("Transformation {}: skipping transformation of {} by filter", name, ingest);
      context.confirm();
    } else {
      doTransform(context, ingest);
    }
    return true;
  }

  @Override
  public void onIdle(OnIdleContext context) {
    Metrics.reportConsumerWatermark(name, context.getWatermark(), -1);
  }

  private void doTransform(OffsetCommitter committer, StreamElement ingest) {

    AtomicInteger toConfirm = new AtomicInteger(0);
    try {
      ElementWiseTransformation.Collector<StreamElement> collector =
          elem -> {
            try {
              log.debug("Transformation {}: writing transformed element {}", name, elem);
              ingestRequest(
                  direct(),
                  elem,
                  elem.getUuid(),
                  supportTransactions,
                  rpc -> {
                    if (rpc.getStatus() == 200) {
                      if (toConfirm.decrementAndGet() == 0) {
                        committer.confirm();
                      }
                    } else {
                      toConfirm.set(-1);
                      committer.fail(
                          new RuntimeException(
                              String.format(
                                  "Received invalid status %d:%s",
                                  rpc.getStatus(), rpc.getStatusMessage())));
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

  private DirectDataOperator direct() {
    if (direct == null) {
      direct = repoFactory.apply().getOrCreateOperator(DirectDataOperator.class);
    }
    return direct;
  }
}
