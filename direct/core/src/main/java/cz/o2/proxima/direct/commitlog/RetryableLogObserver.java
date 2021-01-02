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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import lombok.extern.slf4j.Slf4j;

/**
 * {@code LogObserver} which is able to retry the observation on error. The number of retries is
 * configurable.
 */
@Stable
@Slf4j
public class RetryableLogObserver extends AbstractRetryableLogObserver implements LogObserver {

  /**
   * Create online retryable log observer.
   *
   * @param numRetries number of allowed successive failures
   * @param name name of the consumer
   * @param reader the {@link CommitLogReader}
   * @param observer observer of data
   * @return the observer
   */
  public static RetryableLogObserver online(
      int numRetries, String name, CommitLogReader reader, LogObserver observer) {

    return new RetryableLogObserver(numRetries, name, reader, false, observer);
  }

  /**
   * Create bulk retryable log observer.
   *
   * @param numRetries number of allowed successive failures
   * @param name name of the consumer
   * @param reader the {@link CommitLogReader}
   * @param observer consumer of data
   * @return the observer
   */
  public static RetryableLogObserver bulk(
      int numRetries, String name, CommitLogReader reader, LogObserver observer) {

    return new RetryableLogObserver(numRetries, name, reader, true, observer);
  }

  private final LogObserver observer;
  private final boolean bulk;

  private RetryableLogObserver(
      int maxRetries, String name, CommitLogReader commitLog, boolean bulk, LogObserver observer) {

    super(maxRetries, name, commitLog);
    this.bulk = bulk;
    this.observer = observer;
  }

  @Override
  public final boolean onNext(StreamElement ingest, OnNextContext context) {

    boolean ret = observer.onNext(ingest, context);
    success();
    return ret;
  }

  @Override
  protected final ObserveHandle startInternal(Position position) {
    log.info(
        "Starting to process commitlog {} as {} from {}",
        getCommitLog().getUri(),
        getName(),
        position);
    if (bulk) {
      return getCommitLog().observeBulk(getName(), position, this);
    }
    return getCommitLog().observe(getName(), position, this);
  }

  @Override
  protected final void failure(Throwable error) {
    observer.onError(error);
  }

  @Override
  public void onCompleted() {
    observer.onCompleted();
  }

  @Override
  public void onCancelled() {
    observer.onCancelled();
  }

  @Override
  public void onRepartition(OnRepartitionContext context) {
    observer.onRepartition(context);
  }

  @Override
  public void onIdle(OnIdleContext context) {
    observer.onIdle(context);
  }
}
