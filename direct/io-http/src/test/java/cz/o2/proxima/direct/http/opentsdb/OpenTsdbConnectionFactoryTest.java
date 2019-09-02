/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link OpenTsdbConnectionFactory}. */
public class OpenTsdbConnectionFactoryTest {

  final Repository repo = Repository.of(ConfigFactory.load("test-reference.conf"));
  final EntityDescriptor gateway =
      repo.findEntity("gateway")
          .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
  final AttributeDescriptor metric =
      gateway
          .findAttribute("metric")
          .orElseThrow(() -> new IllegalStateException("Missing attribute metric"));

  URL openedUri;
  ByteArrayOutputStream payload;
  int newConnections;

  OpenTsdbConnectionFactory factory =
      new OpenTsdbConnectionFactory() {
        @Override
        public HttpURLConnection newConnection(URL base) throws IOException {
          newConnections++;
          openedUri = base;
          HttpURLConnection ret = mock(HttpURLConnection.class);
          when(ret.getOutputStream()).thenReturn(payload);
          return ret;
        }
      };

  @Before
  public void setUp() {
    openedUri = null;
    payload = new ByteArrayOutputStream();
    newConnections = 0;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOpenConnection() throws IOException, URISyntaxException {
    factory.openConnection(
        new URI("opentsdb://test"),
        StreamElement.update(
            gateway,
            metric,
            UUID.randomUUID().toString(),
            "key",
            metric.getName(),
            1234567890000L,
            metric.getValueSerializer().serialize(1.1f)));
    assertEquals("http://test/api/put", openedUri.toString());
    assertEquals(
        "{\"metric\": \"key\",\"timestamp\": 1234567890000,\"value\": 1.1,\"tags\": "
            + "{\"entity\": \"gateway\",\"attribute\": \"metric\"}}",
        payload.toString());
    assertEquals(1, newConnections);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOpenConnection2() throws IOException, URISyntaxException {
    factory.openConnection(
        new URI("opentsdb://test"),
        StreamElement.update(
            gateway,
            metric,
            UUID.randomUUID().toString(),
            "key",
            metric.getName(),
            1234567890000L,
            metric.getValueSerializer().serialize(1.1f)));
    assertEquals("http://test/api/put", openedUri.toString());
    assertEquals(
        "{\"metric\": \"key\",\"timestamp\": 1234567890000,\"value\": 1.1,\"tags\": "
            + "{\"entity\": \"gateway\",\"attribute\": \"metric\"}}",
        payload.toString());
    payload.reset();
    factory.openConnection(
        new URI("opentsdb://test"),
        StreamElement.update(
            gateway,
            metric,
            UUID.randomUUID().toString(),
            "key",
            metric.getName(),
            1234567890000L,
            metric.getValueSerializer().serialize(1.1f)));
    assertEquals("http://test/api/put", openedUri.toString());
    assertEquals(
        "{\"metric\": \"key\",\"timestamp\": 1234567890000,\"value\": 1.1,\"tags\": "
            + "{\"entity\": \"gateway\",\"attribute\": \"metric\"}}",
        payload.toString());
    assertEquals(2, newConnections);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOpenConnectionDifferentBases() throws IOException, URISyntaxException {
    factory.openConnection(
        new URI("opentsdb://test"),
        StreamElement.update(
            gateway,
            metric,
            UUID.randomUUID().toString(),
            "key",
            metric.getName(),
            1234567890000L,
            metric.getValueSerializer().serialize(1.1f)));
    assertEquals("http://test/api/put", openedUri.toString());
    assertEquals(
        "{\"metric\": \"key\",\"timestamp\": 1234567890000,\"value\": 1.1,\"tags\": "
            + "{\"entity\": \"gateway\",\"attribute\": \"metric\"}}",
        payload.toString());
    payload.reset();
    factory.openConnection(
        new URI("opentsdb://test2"),
        StreamElement.update(
            gateway,
            metric,
            UUID.randomUUID().toString(),
            "key",
            metric.getName(),
            1234567890000L,
            metric.getValueSerializer().serialize(1.1f)));
    assertEquals("http://test2/api/put", openedUri.toString());
    assertEquals(
        "{\"metric\": \"key\",\"timestamp\": 1234567890000,\"value\": 1.1,\"tags\": "
            + "{\"entity\": \"gateway\",\"attribute\": \"metric\"}}",
        payload.toString());
    assertEquals(2, newConnections);
  }
}
