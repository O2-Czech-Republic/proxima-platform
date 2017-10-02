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

package cz.o2.proxima.storage.kafka;

import java.net.URI;

/**
 * Various utilities.
 */
public class Utils {
  
  public static String topic(URI uri) {
    String topic = uri.getPath().substring(1);
    while (topic.endsWith("/")) {
      topic = topic.substring(0, topic.length());
    }    
    if (topic.isEmpty()) {
      throw new IllegalArgumentException("Invalid path in URI " + uri);
    }
    return topic;
  }
  
  private Utils() { }

}
