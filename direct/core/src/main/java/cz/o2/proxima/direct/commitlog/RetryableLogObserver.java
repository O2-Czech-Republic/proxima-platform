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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * {@code LogObserver} which is able to retry the observation on error. The number of retries is
 * configurable.
 */
@Stable
@Slf4j
public class RetryableLogObserver implements LogObserver {

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

  /** Maximal number of retries. */
  @Getter private final int maxRetries;
  /** Name of the consumer. */
  @Getter private final String name;
  /** The commit log this observer observes from. */
  @Getter private final CommitLogReader commitLog;
  /** Current number of failures in a row. */
  private int numFailures;
  /** Underlying log observer. */
  private final LogObserver delegate;
  /** Whether observer is online or bulk. */
  private final boolean bulk;

  private RetryableLogObserver(
      int maxRetries, String name, CommitLogReader commitLog, boolean bulk, LogObserver delegate) {
    this.maxRetries = maxRetries;
    this.name = name;
    this.commitLog = commitLog;
    this.bulk = bulk;
    this.delegate = delegate;
  }

  @Override
  public final boolean onNext(StreamElement ingest, OnNextContext context) {
    boolean ret = delegate.onNext(ingest, context);
    numFailures = 0;
    return ret;
  }

  @Override
  public boolean onError(Throwable throwable) {
    if (delegate.onError(throwable)) {
      numFailures++;
      log.error(
          "Error in observing commit log {} by {}, retries so far {}, maxRetries {}",
          commitLog.getUri(),
          name,
          numFailures,
          maxRetries,
          throwable);
      return numFailures < maxRetries;
    }
    log.error(
        "Error in observing commit log {} by {} (non-retryable)",
        commitLog.getUri(),
        name,
        throwable);
    return false;
  }

  @Override
  public void onCompleted() {
    delegate.onCompleted();
  }

  @Override
  public void onCancelled() {
    delegate.onCancelled();
  }

  @Override
  public void onRepartition(OnRepartitionContext context) {
    delegate.onRepartition(context);
  }

  @Override
  public void onIdle(OnIdleContext context) {
    delegate.onIdle(context);
  }

  public ObserveHandle start() {
    return start(Position.NEWEST);
  }

  public ObserveHandle start(Position position) {
    return startInternal(position);
  }

  /**
   * Called when processing is to start from given position.
   *
   * @param position position in the log
   * @return handle of the observe process
   */
  protected final ObserveHandle startInternal(Position position) {
    log.info(
        "Starting to process commit-log {} as {} from {}",
        getCommitLog().getUri(),
        getName(),
        position);
    if (bulk) {
      return getCommitLog().observeBulk(getName(), position, this);
    }
    return getCommitLog().observe(getName(), position, this);
  }
}
