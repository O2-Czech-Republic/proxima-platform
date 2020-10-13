/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.util;

import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.UnaryFunction;
import java.io.Serializable;

/** Utilities related to exception throwing and handling. */
public class ExceptionUtils {

  /**
   * Consumer throwing exceptions.
   *
   * @param <T> type parameter
   */
  @FunctionalInterface
  public interface ThrowingConsumer<T> extends Serializable {
    void apply(T what) throws Exception;
  }

  /** Runnable throwing exception. */
  @FunctionalInterface
  public interface ThrowingRunnable extends Serializable {
    void run() throws Exception;
  }

  /**
   * Factory throwing exception.
   *
   * @param <T> type parameter
   */
  @FunctionalInterface
  public interface ThrowingFactory<T> extends Serializable {
    T get() throws Exception;
  }

  /**
   * Function throwing exception.
   *
   * @param <IN> input type parameter
   * @param <OUT> output type parameter
   */
  @FunctionalInterface
  public interface ThrowingUnaryFunction<IN, OUT> extends Serializable {
    OUT apply(IN what) throws Exception;
  }

  /**
   * Wrap consumer throwing exceptions to regular consumer.
   *
   * @param <T> type parameter
   * @param wrap the consumer to wrap
   * @return regular consumer
   */
  public static <T> Consumer<T> uncheckedConsumer(ThrowingConsumer<T> wrap) {
    return t -> {
      try {
        wrap.apply(t);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  /**
   * Run given runnable rethrowing exceptions as {@link RuntimeException}s.
   *
   * @param runnable the runnable to run
   */
  public static void unchecked(ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Run given factory and return result.
   *
   * @param <T> type parameter
   * @param factory the factory that throws exceptions
   * @return created instance of the factory
   */
  public static <T> T uncheckedFactory(ThrowingFactory<T> factory) {
    try {
      return factory.get();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Wrap throwing unary function as regular one.
   *
   * @param <IN> input type parameter
   * @param <OUT> output type parameter
   * @param fn the function to wrap
   * @return regular unary function
   */
  public static <IN, OUT> UnaryFunction<IN, OUT> uncheckedFn(ThrowingUnaryFunction<IN, OUT> fn) {

    return in -> {
      try {
        return fn.apply(in);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    };
  }

  /**
   * Run given runnable, swallow any potential {@link InterruptedException} and set current thread's
   * interrupted flag (if exception caught).
   *
   * @param runnable runnable throwing {@link InterruptedException}
   * @return {@code true} if {@link InterruptedException} was caught
   */
  public static boolean ignoringInterrupted(ThrowingRunnable runnable) {
    try {
      runnable.run();
      return false;
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      return true;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /** Verify that given {@link Throwable} (or any of its causes) is {@link InterruptedException}. */
  public static boolean isInterrupted(Throwable ex) {
    return ex instanceof InterruptedException
        || ex.getCause() != null && isInterrupted(ex.getCause());
  }

  private ExceptionUtils() {}
}
