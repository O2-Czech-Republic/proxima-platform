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
package cz.o2.proxima.storage;

import cz.o2.proxima.annotations.Internal;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utilities related to URIs. */
@Internal
public class UriUtil {

  /**
   * Parse query string from URI
   *
   * @param uri uri for parsing
   * @return Map of query params
   */
  public static Map<String, String> parseQuery(URI uri) {
    String query = uri.getRawQuery();
    if (query == null) {
      return Collections.emptyMap();
    }
    return Arrays.asList(query.split("&"))
        .stream()
        .map(s -> Arrays.copyOf(s.split("="), 2))
        .collect(Collectors.toMap(s -> decode(s[0]), s -> decode(s[1])));
  }

  private static String decode(final String encoded) {
    try {
      return encoded == null ? null : URLDecoder.decode(encoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("UTF-8 is a required encoding", e);
    }
  }

  /**
   * Get normalized path from URI, which:
   *
   * <ul>
   *   <li>is not null
   *   <li>doesn't start or end with slash
   * </ul>
   *
   * @param uri the URI to extract path from
   * @return normalized path
   */
  public static String getPathNormalized(URI uri) {
    String p = uri.getPath();
    while (p.startsWith("/")) {
      p = p.substring(1);
    }
    while (p.endsWith("/")) {
      p = p.substring(0, p.length() - 1);
    }
    return p;
  }

  /**
   * Parse path from URI
   *
   * @param uri uri for parsing
   * @return list of paths as string
   */
  public static List<String> parsePath(URI uri) {
    String path = uri.getRawPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    if (path.isEmpty()) {
      return Collections.emptyList();
    }
    return Arrays.stream(path.split("/")).map(UriUtil::decode).collect(Collectors.toList());
  }

  private UriUtil() {}
}
