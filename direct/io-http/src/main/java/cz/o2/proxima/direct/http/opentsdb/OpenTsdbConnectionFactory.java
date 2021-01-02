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
package cz.o2.proxima.direct.http.opentsdb;

import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.direct.http.ConnFactory;
import cz.o2.proxima.direct.http.TestableConnFactory;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

/** A {@link ConnFactory} for OpenTSDB. */
@Slf4j
@Experimental("Missing production use-case")
public class OpenTsdbConnectionFactory extends TestableConnFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public HttpURLConnection openConnection(URI base, StreamElement elem) throws IOException {

    if (!elem.getParsed().isPresent()) {
      return null;
    }
    HttpURLConnection conn = createConnection(base);
    @SuppressWarnings("unchecked")
    ValueSerializer valueSerializer = elem.getAttributeDescriptor().getValueSerializer();

    @SuppressWarnings("unchecked")
    String data =
        "{\"metric\": \""
            + elem.getKey()
            + "\","
            + "\"timestamp\": "
            + elem.getStamp()
            + ","
            + "\"value\": "
            + valueSerializer.asJsonValue(elem.getParsed().get())
            + ","
            + "\"tags\": {\"entity\": \""
            + elem.getEntityDescriptor().getName()
            + "\","
            + "\"attribute\": \""
            + elem.getAttribute()
            + "\"}"
            + "}";
    conn.setDoOutput(true);
    conn.getOutputStream().write(data.getBytes(StandardCharsets.UTF_8));
    return conn;
  }

  private HttpURLConnection createConnection(URI base) throws IOException {
    URL url = new URL("http", base.getHost(), base.getPort(), "/api/put");
    log.debug("Request GET on {}", url);
    HttpURLConnection conn = newConnection(url);
    conn.setRequestMethod("POST");
    return conn;
  }
}
