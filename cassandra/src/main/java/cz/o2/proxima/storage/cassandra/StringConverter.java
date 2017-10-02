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

package cz.o2.proxima.storage.cassandra;

import java.nio.charset.Charset;
import javax.annotation.Nullable;

/**
 * A converter between a specified java type and {@code String}.
 */
public interface StringConverter<T> {

  public static final StringConverter<String> DEFAULT = new StringConverter<String>() {

    /** String value that all strings should be less than. */
    final String MAX = new String(new byte[] { (byte) 0xFF }, 0, 1, Charset.forName("ascii"));
    /** String value that all strings should be greater or equal to. */
    final String MIN = "";
    
    @Override
    public String toString(String what) {
      return (String) what;
    }

    @Override
    public @Nullable String fromString(String what) {
      return what;
    }

    @Override
    public String max() {
      return MAX;
    }

    @Override
    public String min() {
      return MIN;
    }

  };

  /**
   * Convert type to string.
   */
  String toString(T what);


  /**
   * Convert type from string.
   */
  T fromString(String what);

  /**
   * Retrieve maximal element.
   */
  T max();

  /**
   * Retrieve minimal element.
   */
  T min();

}
