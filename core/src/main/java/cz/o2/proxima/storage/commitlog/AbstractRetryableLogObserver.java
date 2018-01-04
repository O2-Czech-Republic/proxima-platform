/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cz.o2.proxima.storage.commitlog;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A parent class for retryable online and bulk log observers.
 */
@Slf4j
public abstract class AbstractRetryableLogObserver {

  /** Maximal number of retries. */
  @Getter
  private final int maxRetries;
  /** Name of the consumer. */
  @Getter
  private final String name;
  /** The commit log this observer observes from. */
  @Getter
  private final CommitLogReader commitLog;
  /** Current number of failures in a row. */
  private int numFailures;

  @Getter
  private CommitLogReader.Position position;

  public AbstractRetryableLogObserver(
      int maxRetries,
      String name,
      CommitLogReader commitLog) {

    this.maxRetries = maxRetries;
    this.name = name;
    this.commitLog = commitLog;
  }


  public void onError(Throwable error) {
    log.error(
        "Error in observing commit log {} by {}",
        commitLog.getURI(), name, error);
    if (numFailures++ < maxRetries) {
      startInternal(position);
    } else {
      failure();
    }
  }
  
  protected void success() {
    numFailures = 0;    
  }

  public void start() {
    start(CommitLogReader.Position.NEWEST);
  }

  public void start(CommitLogReader.Position position) {
    this.position = position;
    this.startInternal(position);
  }

  /**
   * Called when processing is to start from given position.
   */
  protected abstract void startInternal(CommitLogReader.Position position);

  /**
   * Called when unrecoverable error detected on the commit log.
   */
  protected abstract void failure();

}
