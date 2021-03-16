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
package cz.o2.proxima.transaction;

import cz.o2.proxima.scheme.ValueSerializerFactory;
import java.io.Serializable;

/**
 * A provider of schemes for {@link Request}, {@link Response} and {@link State}. The provided
 * scheme strings has to be subsequently recognized by {@link ValueSerializerFactory} and provide
 * expected serialization.
 */
public interface TransactionSerializerSchemeProvider extends Serializable {

  static TransactionSerializerSchemeProvider of(
      String requestScheme, String responseScheme, String stateScheme) {
    return new TransactionSerializerSchemeProvider() {
      @Override
      public String getRequestScheme() {
        return requestScheme;
      }

      @Override
      public String getResponseScheme() {
        return responseScheme;
      }

      @Override
      public String getStateScheme() {
        return stateScheme;
      }
    };
  }

  /**
   * Retrieve scheme for serialization of {@link Request}
   *
   * @return the string scheme representing {@link Request} serialization.
   */
  String getRequestScheme();

  /**
   * Retrieve scheme for serialization of {@link Request}
   *
   * @return the string scheme representing {@link Request} serialization.
   */
  String getResponseScheme();

  /**
   * Retrieve scheme for serialization of {@link Request}
   *
   * @return the string scheme representing {@link Request} serialization.
   */
  String getStateScheme();
}
