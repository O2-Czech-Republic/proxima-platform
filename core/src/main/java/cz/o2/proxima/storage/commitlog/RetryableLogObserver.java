/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.storage.commitlog;

import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code LogObserver} which is able to retry the observation on error.
 * The number of retries is configurable.
 */
public abstract class RetryableLogObserver
    extends AbstractRetryableLogObserver implements LogObserver {

  private static final Logger LOG = LoggerFactory.getLogger(RetryableLogObserver.class);


  public RetryableLogObserver(
      int maxRetries,
      String name,
      CommitLogReader commitLog) {

    super(maxRetries, name, commitLog);
  }

  @Override
  public final boolean onNext(
      StreamElement ingest, Partition partition,
      LogObserver.ConfirmCallback confirm) {

    boolean ret = onNextInternal(ingest, partition, confirm);
    success();
    return ret;
  }

  @Override
  protected final void startInternal(CommitLogReader.Position position) {
    LOG.info(
        "Starting to process commitlog {} as {} from {}",
        getCommitLog().getURI(), getName(), getPosition());
    getCommitLog().observe(getName(), getPosition(), this);
  }

  @Override
  public void close() {
    try {
      getCommitLog().close();
    } catch (IOException ex) {
      LOG.error("Error while closing log observer {}", getName(), ex);
    }
  }


  /**
   * Called to observe the ingest data.
   * @returns true to continue processing, false otherwise
   */
  protected boolean onNextInternal(
      StreamElement ingest, LogObserver.ConfirmCallback confirm) {

    throw new UnsupportedOperationException(
        "Please override either of `onNextInternal` methods");
  }


  /**
   * Called to observe the ingest data.
   * @returns true to continue processing, false otherwise
   */
  protected boolean onNextInternal(
      StreamElement ingest, Partition partition,
      LogObserver.ConfirmCallback confirm) {

    return onNextInternal(ingest, confirm);
  }



}
