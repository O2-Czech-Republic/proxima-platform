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

import com.google.common.base.MoreObjects;
import groovy.lang.Closure;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.groovy.reflection.CachedClass;
import org.codehaus.groovy.reflection.CachedMethod;
import org.codehaus.groovy.reflection.ReflectionCache;

/** Various type-related utilities. */
@Slf4j
public class Types {

  /**
   * Retrieve class object of return type of given {@link Closure}.
   *
   * @param <T> type parameter
   * @param closure the closure
   * @return {@link Class} object of given closure return type.
   */
  @SuppressWarnings("unchecked")
  public static <T> Class<T> returnClass(Closure<T> closure) {
    CachedClass cachedClass = ReflectionCache.getCachedClass(closure.getClass());
    Class<T> res = null;
    for (CachedMethod m : cachedClass.getMethods()) {
      if ("call".equals(m.getName())) {
        res = MoreObjects.firstNonNull(res, m.getReturnType());
      }
    }
    for (CachedMethod m : cachedClass.getMethods()) {
      if ("doCall".equals(m.getName())) {
        res = MoreObjects.firstNonNull(res, m.getReturnType());
      }
    }
    return (Class) Object.class;
  }

  private Types() {}
}
