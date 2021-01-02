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
package cz.o2.proxima.tools.groovy.util;

import com.google.common.base.Preconditions;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import groovy.lang.Closure;

/** Utilities related to creating {@link Closure}s from Java. */
public class Closures {

  /**
   * Create {@link Closure} from provided java lambda.
   *
   * @param owner owner of the resulting {@link Closure}
   * @param lambda lambda supplier of resulting objects
   * @param <T> type parameter
   * @return the closure
   */
  public static <T> Closure<T> from(Object owner, Factory<T> lambda) {
    return new Closure<T>(owner) {
      @Override
      public T call() {
        return lambda.apply();
      }
    };
  }

  /**
   * Create {@link Closure} from provided java lambda.
   *
   * @param owner owner of the resulting {@link Closure}
   * @param lambda lambda function from Object to output type
   * @param <T> type parameter
   * @return the closure
   */
  public static <T> Closure<T> from(Object owner, UnaryFunction<Object, T> lambda) {
    return new Closure<T>(owner) {
      @Override
      public T call(Object arg) {
        return lambda.apply(arg);
      }
    };
  }

  /**
   * Create {@link Closure} from provided java lambda.
   *
   * @param owner owner of the resulting {@link Closure}
   * @param lambda lambda function of two arguments to output type
   * @param <T> type parameter
   * @return the closure
   */
  public static <T> Closure<T> from(Object owner, BiFunction<Object, Object, T> lambda) {
    return new Closure<T>(owner) {
      @Override
      public T call(Object... args) {
        Preconditions.checkArgument(
            args.length == 2, "Need exactly two arguments, got ", args.length);
        return lambda.apply(args[0], args[1]);
      }
    };
  }

  /**
   * Create {@link Closure} from provided java lambda.
   *
   * @param owner owner of the resulting {@link Closure}
   * @param lambda lambda function from Object[] to output type
   * @param <T> type parameter
   * @return the closure
   */
  public static <T> Closure<T> fromArray(Object owner, UnaryFunction<Object[], T> lambda) {
    return new Closure<T>(owner) {
      @Override
      public T call(Object... args) {
        return lambda.apply(args);
      }
    };
  }

  // do not construct
  private Closures() {}
}
