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
import cz.o2.proxima.storage.commitlog.Position;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** A parent class for retryable online and bulk log observers. */
@Stable
@Slf4j
public abstract class AbstractRetryableLogObserver implements LogObserver {

  private static final long serialVersionUID = 1L;

  /** Maximal number of retries. */
  @Getter private final int maxRetries;
  /** Name of the consumer. */
  @Getter private final String name;
  /** The commit log this observer observes from. */
  private final CommitLogReader commitLog;
  /** Current number of failures in a row. */
  private int numFailures;

  public AbstractRetryableLogObserver(int maxRetries, String name, CommitLogReader commitLog) {

    this.maxRetries = maxRetries;
    this.name = name;
    this.commitLog = commitLog;
  }

  @Override
  public boolean onError(Throwable error) {
    numFailures++;
    log.error(
        "Error in observing commit log {} by {}, retries so far {}, maxRetries {}",
        commitLog.getUri(),
        name,
        numFailures,
        maxRetries,
        error);
    if (numFailures < maxRetries) {
      return true;
    } else {
      failure(error);
      return false;
    }
  }

  protected void success() {
    numFailures = 0;
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
  protected abstract ObserveHandle startInternal(Position position);

  /**
   * Called when unrecoverable error detected on the commit log.
   *
   * @param lastError the last error thrown during processing
   */
  protected abstract void failure(Throwable lastError);

  @SuppressWarnings("unchecked")
  CommitLogReader getCommitLog() {
    return commitLog;
  }
}
