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
package cz.o2.proxima.util;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Pattern matcher for syntax used for patterns in input config.
 * Accepted patterns can contain wildcards (*) which are then translated
 * into {@code java.util.regex.Pattern}s.
 */
public class NamePattern implements Serializable {

  private final String pattern;
  private final Pattern compiled;

  /**
   * Constructor.
   * Convert ingest config pattern into java {@code java.util.regex.Pattern}.
   * @param pattern string pattern to wrap into this object
   */
  public NamePattern(String pattern) {
    this.pattern = Objects.requireNonNull(pattern);
    this.compiled = convert(pattern);
  }

  private Pattern convert(String pattern) {
    String textPattern = pattern.replace(".", "\\.").replace("*", ".+");
    return Pattern.compile("^" + textPattern + "$");
  }

  /**
   * Match input string against the pattern.
   * @param what the string to match
   * @return {@code true} if matches
   */
  public boolean matches(String what) {
    return compiled.matcher(what).find();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof NamePattern)) {
      return false;
    }
    NamePattern other = (NamePattern) obj;
    return other.pattern.equals(this.pattern);
  }

  @Override
  public int hashCode() {
    return pattern.hashCode();
  }

}
