/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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

import cz.o2.proxima.repository.Repository;
import static cz.o2.proxima.server.IngestServer.die;
import static cz.o2.proxima.server.IngestServer.ingestRequest;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.RetryableLogObserver;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.transform.Transformation;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * Observer of source data performing transformation to another entity/attribute.
 */
@Slf4j
public class TransformationObserver extends RetryableLogObserver {

  private final Repository repo;
  private final DirectDataOperator direct;
  private final Transformation transformation;
  private final StorageFilter filter;
  private final String name;

  TransformationObserver(
      int retries, String consumer, CommitLogReader reader,
      Repository repo, DirectDataOperator direct,
      String name, Transformation transformation,
      StorageFilter filter) {


    super(retries, consumer, reader);
    this.repo = repo;
    this.direct = direct;
    this.name = name;
    this.transformation = transformation;
    this.filter = filter;
  }

  @Override
  protected void failure() {
    die(String.format(
        "Failed to transform using %s. Bailing out.",
        transformation));
  }

  @Override
  public boolean onNextInternal(
      StreamElement ingest, OffsetCommitter committer) {

    if (!filter.apply(ingest)) {
      log.debug(
          "Transformation {}: skipping transformation of {} by filter",
          name,  ingest);
      committer.confirm();
    } else {
      doTransform(committer, ingest);
    }
    return true;
  }

  private void doTransform(
      OffsetCommitter committer, StreamElement ingest) {

    AtomicInteger toConfirm = new AtomicInteger(0);
    try {
      Transformation.Collector<StreamElement> collector = elem -> {
        try {
          log.debug(
              "Transformation {}: writing transformed element {}",
              name, elem);
          ingestRequest(
              direct, elem, elem.getUuid(), rpc -> {
                if (rpc.getStatus() == 200) {
                  if (toConfirm.decrementAndGet() == 0) {
                    committer.confirm();
                  }
                } else {
                  toConfirm.set(-1);
                  committer.fail(new RuntimeException(
                      String.format("Received invalid status %d:%s",
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

}
