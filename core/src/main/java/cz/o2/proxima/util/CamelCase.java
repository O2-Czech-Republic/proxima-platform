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

import com.google.common.collect.Sets;
import java.util.Set;

/** Transform string to camel case. */
public class CamelCase {

  public enum Characters {
    SPACE_AND_DASH(Sets.newHashSet(' ', '-')),
    SPACE_DASH_AND_UNDERSCORE(Sets.newHashSet(' ', '-', '_'));

    private final Set<Character> set;

    Characters(Set<Character> set) {
      this.set = set;
    }
  }

  /**
   * Convert given string to camelCase with first letter upper case.
   *
   * @param what the string to convert
   * @return the converted string
   */
  public static String apply(String what) {
    return CamelCase.apply(what, true, Characters.SPACE_AND_DASH);
  }

  /**
   * Convert given string to camelCase with first letter upper case.
   *
   * @param what the string to convert
   * @param camelSeparators chars that separate words in camel
   * @return the converted string
   */
  public static String apply(String what, Characters camelSeparators) {
    return CamelCase.apply(what, true, camelSeparators);
  }

  /**
   * Convert given string to camelCase.
   *
   * @param what the string to convert
   * @param startWithCapital should the returned string start with upper (true) or lower case
   *     (false)
   * @return the converted string
   */
  public static String apply(String what, boolean startWithCapital) {
    return apply(what, startWithCapital, Characters.SPACE_AND_DASH);
  }

  /**
   * Convert given string to camelCase.
   *
   * @param what the string to convert
   * @param startWithCapital should the returned string start with upper (true) or lower case
   *     (false)
   * @param camelSeparators chars that separate words in camel
   * @return the converted string
   */
  public static String apply(String what, boolean startWithCapital, Characters camelSeparators) {
    if (what.isEmpty()) {
      return what;
    }
    StringBuilder sb = new StringBuilder();
    boolean nextUpper = startWithCapital;
    for (char c : what.toCharArray()) {
      if (camelSeparators.set.contains(c)) {
        nextUpper = true;
      } else if (nextUpper) {
        sb.append(Character.toUpperCase(c));
        nextUpper = false;
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private CamelCase() {}
}
