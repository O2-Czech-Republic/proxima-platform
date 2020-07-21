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
package cz.o2.proxima.direct.http;

import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Classpath;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

/** Writer via HTTP(S) requests. */
public class HttpWriter extends AbstractStorage implements OnlineAttributeWriter, DataAccessor {

  private static final long serialVersionUID = 1L;

  private final ConnFactory connFactory;
  @Getter private final Map<String, Object> cfg;

  public HttpWriter(EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

    super(entityDesc, uri);
    try {
      this.connFactory = getConnFactory(cfg);
    } catch (InstantiationException | IllegalAccessException ex) {
      throw new RuntimeException(ex);
    }
    this.cfg = cfg;
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {

    HttpURLConnection conn = null;
    try {
      conn = connFactory.openConnection(getUri(), data);
      if (conn != null) {
        int code = conn.getResponseCode();
        // discard any response data
        conn.getInputStream().close();
        if (code / 100 == 2) {
          statusCallback.commit(true, null);
        } else {
          statusCallback.commit(false, new RuntimeException("Invalid status code " + code));
        }
      } else {
        statusCallback.commit(true, null);
      }
    } catch (Exception ex) {
      statusCallback.commit(false, ex);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  @Override
  public Factory<?> asFactory() {
    final EntityDescriptor entity = getEntityDescriptor();
    final URI uri = getUri();
    final Map<String, Object> cfg = this.cfg;
    return repo -> new HttpWriter(entity, uri, cfg);
  }

  protected ConnFactory getConnFactory(Map<String, Object> cfg)
      throws InstantiationException, IllegalAccessException {

    String factory = (String) cfg.get("connectionFactory");
    if (factory == null) {
      return (uri, elem) -> (HttpURLConnection) uri.toURL().openConnection();
    }
    Class<? extends ConnFactory> cls = Classpath.findClass(factory, ConnFactory.class);
    return cls.newInstance();
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(this);
  }

  @Override
  public void close() {
    // nop
  }
}
