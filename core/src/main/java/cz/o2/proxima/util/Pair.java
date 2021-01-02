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
package cz.o2.proxima.util;

import cz.o2.proxima.annotations.Internal;
import java.io.Serializable;
import java.util.Objects;
import lombok.Getter;

/** A generic pair (tuple). */
@Internal
public class Pair<A, B> implements Serializable {

  private static final long serialVersionUID = 1L;

  public static <A, B> Pair<A, B> of(A first, B second) {
    return new Pair<>(first, second);
  }

  @Getter final A first;

  @Getter final B second;

  private Pair(A a, B b) {
    this.first = a;
    this.second = b;
  }

  @Override
  public String toString() {
    return "Pair(first=" + first + ", second=" + second + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof Pair) {
      Pair<?, ?> other = (Pair) obj;
      return Objects.equals(other.first, first) && Objects.equals(other.second, second);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }
}
