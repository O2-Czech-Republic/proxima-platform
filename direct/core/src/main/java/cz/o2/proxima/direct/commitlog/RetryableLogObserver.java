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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.storage.StreamElement;
import lombok.extern.slf4j.Slf4j;


/**
 * {@code LogObserver} which is able to retry the observation on error.
 * The number of retries is configurable.
 */
@Stable
@Slf4j
public abstract class RetryableLogObserver
    extends AbstractRetryableLogObserver implements LogObserver {

  public RetryableLogObserver(
      int maxRetries,
      String name,
      CommitLogReader commitLog) {

    super(maxRetries, name, commitLog);
  }

  @Override
  public final boolean onNext(StreamElement ingest, OnNextContext context) {

    boolean ret = onNextInternal(ingest, context);
    success();
    return ret;
  }

  @Override
  protected final ObserveHandle startInternal(Position position) {
    log.info(
        "Starting to process commitlog {} as {} from {}",
        getCommitLog().getUri(), getName(), getPosition());
    return getCommitLog().observe(getName(), getPosition(), this);
  }

  /**
   * Called to observe the ingest data.
   * @param ingest input data
   * @param context context of the element
   * @return {@code true} to continue processing, {@code false} otherwise
   */
  protected abstract boolean onNextInternal(
      StreamElement ingest, OnNextContext context);

}
