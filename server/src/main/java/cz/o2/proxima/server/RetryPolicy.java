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

package cz.o2.proxima.server;

import java.util.function.Consumer;

/**
 * A retry policy applied to wherever a retry is needed.
 */
@FunctionalInterface
public interface RetryPolicy {
  
  /** Exception thrown when all retries specified by policy has been exhausted. */
  public static class RetriesExhausted extends Exception {
    public RetriesExhausted() {
      super("Exhausted retries trying to execute command");
    }
    public RetriesExhausted(String message) {
      super(message);
    }
    public RetriesExhausted(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Asynchronously runnable and retriable.
   * The {@code Consumer} should obtain exception on error and {@code null}
   * on success.
   */
  @FunctionalInterface
  public interface AsyncRunnable {

    /** Asynchronously run the command. Use the callback to return exception or null. */
    void run(Consumer<Exception> success);

  }
    
    
  /**
   * Retry given idempotent runnable with given policy.
   * @throws {@code RetriesExhausted} if failed to execute the
   * command within the retry policy.
   * @param what the command to run
   * @param handler the handler to be called when the retry process completes,
   *        {@code true} meaning it was finished with success,
   *        {@code false} signals retries exhausted
   */
  public void retry(AsyncRunnable what, Consumer<Boolean> handler);

}
