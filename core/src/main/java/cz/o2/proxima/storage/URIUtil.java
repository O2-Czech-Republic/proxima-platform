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
package cz.o2.proxima.storage;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities related to URIs.
 */
public class URIUtil {

  public static Map<String, String> parseQuery(URI uri) {
    String query = uri.getQuery();
    return Arrays.asList(query.split("&")).stream()
        .map(s -> Arrays.copyOf(s.split("="), 2))
        .collect(Collectors.toMap(s -> decode(s[0]), s -> decode(s[1])));
  }

  private static String decode(final String encoded) {
    try {
      return encoded == null ? null : URLDecoder.decode(encoded, "UTF-8");
    } catch(final UnsupportedEncodingException e) {
      throw new IllegalStateException("UTF-8 is a required encoding", e);
    }
  }

}
