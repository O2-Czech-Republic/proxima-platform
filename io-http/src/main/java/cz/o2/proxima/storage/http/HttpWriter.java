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
package cz.o2.proxima.storage.http;

import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Classpath;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

/**
 * Writer via HTTP(S) requests.
 */
public class HttpWriter implements OnlineAttributeWriter, DataAccessor {

  final URI uri;
  final ConnFactory connFactory;

  public HttpWriter(
      EntityDescriptor entityDesc,
      URI uri,
      Map<String, Object> cfg) {

    this.uri = uri;
    try {
      this.connFactory = getConnFactory(cfg);
    } catch (ClassNotFoundException
        | InstantiationException | IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public void write(
      StreamElement data,
      CommitCallback statusCallback) {

    HttpURLConnection conn = null;
    try {
      conn = connFactory.openConnection(uri, data);
      int code = conn.getResponseCode();
      if (code == 200) {
        statusCallback.commit(true, null);
      } else {
        statusCallback.commit(false, new RuntimeException("Invalid status code " + code));
      }
    } catch (Exception ex) {
      statusCallback.commit(false, ex);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  private ConnFactory getConnFactory(Map<String, Object> cfg)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {

    String factory = (String) cfg.get("connectionFactory");
    if (factory == null) {
      return (uri, elem) -> {
        return (HttpURLConnection) uri.toURL().openConnection();
      };
    }
    Class<ConnFactory> cls = Classpath.findClass(factory, ConnFactory.class);
    return cls.newInstance();
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(this);
  }



}
