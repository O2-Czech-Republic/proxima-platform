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
package cz.o2.proxima.direct.http;

import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URI;
import javax.annotation.Nullable;

/** A factory for connections based on input elements. */
@FunctionalInterface
public interface ConnFactory extends Serializable {

  /**
   * Open and return HTTP(S) connection to given base URI with given input stream element. The
   * returned connection must be open and ready to retrieve status.
   *
   * @param base URI
   * @param elem input element
   * @return new connection
   * @throws IOException on IO errors
   */
  @Nullable
  HttpURLConnection openConnection(URI base, StreamElement elem) throws IOException;
}
