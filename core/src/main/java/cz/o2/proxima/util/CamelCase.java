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

import java.nio.charset.Charset;

/**
 * Transform string to camel case.
 */
public class CamelCase {

  private static final Charset CHARSET = Charset.forName("UTF-8");


  /**
   * Convert given string to camelCase with first letter upper case.
   * @param what the string to convert
   * @return the converted string
   */
  public static String apply(String what) {
    return CamelCase.apply(what, true);
  }

  /**
   * Convert given string to camelCase.
   * @param what the string to convert
   * @param startWithCapital should the returned string start with upper (true)
   *                         or lower case (false)
   * @return the converted string
   */
  public static String apply(String what, boolean startWithCapital) {
    if (what.isEmpty()) {
      return what;
    }
    StringBuilder sb = new StringBuilder();
    boolean nextUpper = startWithCapital;
    for (char c : what.toCharArray()) {
      if (c == '-' || c == ' ') {
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

}
