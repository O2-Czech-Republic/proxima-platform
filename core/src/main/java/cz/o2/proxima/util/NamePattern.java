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

/**
 * Pattern matcher for syntax used for patterns in input config. Accepted patterns can contain
 * wildcards (*) which are then translated into {@code java.util.regex.Pattern}s.
 */
@Internal
public class NamePattern implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String pattern;
  private final boolean prefixOnly;

  /**
   * Constructor. Convert ingest config pattern into java {@code java.util.regex.Pattern}.
   *
   * @param pattern string pattern to wrap into this object
   */
  public NamePattern(String pattern) {
    this.prefixOnly = pattern.endsWith(".*");
    this.pattern = convert(Objects.requireNonNull(pattern), prefixOnly);
  }

  private String convert(String pattern, boolean prefixOnly) {
    if (prefixOnly) {
      return pattern.substring(0, pattern.length() - 1);
    }
    return pattern;
  }

  /**
   * Match input string against the pattern.
   *
   * @param what the string to match
   * @return {@code true} if matches
   */
  public boolean matches(String what) {
    if (prefixOnly) {
      return what.startsWith(pattern);
    }
    return what.equals(pattern);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof NamePattern)) {
      return false;
    }
    NamePattern other = (NamePattern) obj;
    return other.pattern.equals(this.pattern) && other.prefixOnly == prefixOnly;
  }

  @Override
  public int hashCode() {
    return Objects.hash(pattern, prefixOnly);
  }
}
