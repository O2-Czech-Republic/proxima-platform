/**
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.cassandra;

import cz.o2.proxima.direct.randomaccess.RandomOffset;

/** Offset based on {@code token} function. */
public class Offsets {

  private static RandomOffset EMPTY = new RandomOffset() {};

  /** Offset represented by a token (hash) of the key. */
  public static class Token implements RandomOffset {

    private static final long serialVersionUID = 1L;

    final long tkn;

    Token(long token) {
      this.tkn = token;
    }

    public long getToken() {
      return tkn;
    }
  }

  /** Offset represented by the raw string value. */
  public static class Raw implements RandomOffset {

    private static final long serialVersionUID = 1L;

    final String str;

    Raw(String str) {
      this.str = str;
    }

    public String getRaw() {
      return str;
    }
  }

  public static RandomOffset empty() {
    return EMPTY;
  }

  private Offsets() {
    // nop
  }
}
