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

import static cz.o2.proxima.direct.commitlog.ObserverUtils.asOnNextContext;

import com.google.common.collect.Iterables;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.LogObserver.OnNextContext;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

/** Reader of data from websocket (ws, or wss). */
public class WebsocketReader extends AbstractStorage implements CommitLogReader {

  private static final long serialVersionUID = 1L;

  private static final Partition PARTITION = () -> 0;
  private static final Offset OFFSET =
      new Offset() {

        private static final long serialVersionUID = 1L;

        @Override
        public Partition getPartition() {
          return PARTITION;
        }

        @Override
        public long getWatermark() {
          return System.currentTimeMillis();
        }
      };

  private final AttributeDescriptor<?> attr;
  private final UnaryFunction<String, String> keyExtractor;
  private final String hello;
  private final Map<String, Object> cfg;

  public WebsocketReader(EntityDescriptor entityDescriptor, URI uri, Map<String, Object> cfg) {

    super(entityDescriptor, uri);
    @SuppressWarnings("unchecked")
    List<String> attributes = (List<String>) Objects.requireNonNull(cfg.get("attributes"));
    if (attributes.size() > 1) {
      throw new IllegalArgumentException(
          "Can read only single attribute from websocket, got " + attributes);
    }
    String name = Iterables.getOnlyElement(attributes);
    if (name.equals("*")) {
      if (entityDescriptor.getAllAttributes().size() != 1) {
        throw new IllegalArgumentException(
            "When specifying wildcard attribute, entity has to have "
                + "only single attribute, got "
                + entityDescriptor.getAllAttributes());
      }
      name = Iterables.getOnlyElement(entityDescriptor.getAllAttributes()).getName();
    }
    String attrName = name;
    attr =
        entityDescriptor
            .findAttribute(attrName)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Attribute " + attrName + " should be present in " + entityDescriptor));
    // FIXME: keyExtractor
    keyExtractor = m -> UUID.randomUUID().toString();
    hello =
        Optional.ofNullable(cfg.get("hello"))
            .map(Object::toString)
            .orElseThrow(() -> new IllegalArgumentException("Missing 'hello' message"));
    this.cfg = cfg;
  }

  @Override
  public List<Partition> getPartitions() {
    // single partition
    return Arrays.asList(PARTITION);
  }

  @Override
  public ObserveHandle observe(String name, Position position, LogObserver observer) {

    checkSupportedPosition(position);
    return observe(element -> observer.onNext(element, nullContext()), observer::onError);
  }

  private ObserveHandle observe(Consumer<StreamElement> onNext, Consumer<Throwable> onError) {

    WebSocketClient client =
        new WebSocketClient(getUri()) {

          @Override
          public void onOpen(ServerHandshake sh) {
            send(hello);
          }

          @Override
          public void onMessage(String m) {
            StreamElement elem =
                StreamElement.upsert(
                    getEntityDescriptor(),
                    attr,
                    UUID.randomUUID().toString(),
                    keyExtractor.apply(m),
                    attr.getName(),
                    System.currentTimeMillis(),
                    m.getBytes(StandardCharsets.UTF_8));
            onNext.accept(elem);
          }

          @Override
          public void onClose(int code, String reason, boolean remote) {
            if (remote) {
              onError.accept(new RuntimeException("Server error: " + code + ": " + reason));
            }
          }

          @Override
          public void onError(Exception excptn) {
            onError.accept(excptn);
          }
        };
    client.connect();
    return new ObserveHandle() {
      @Override
      public void close() {
        client.close();
      }

      @Override
      public List<Offset> getCommittedOffsets() {
        return Arrays.asList(OFFSET);
      }

      @Override
      public void resetOffsets(List<Offset> offsets) {
        // nop
      }

      @Override
      public List<Offset> getCurrentOffsets() {
        return getCommittedOffsets();
      }

      @Override
      public void waitUntilReady() throws InterruptedException {
        // nop
      }
    };
  }

  @Override
  public ObserveHandle observePartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    checkSupportedPosition(position);
    return observe(element -> observer.onNext(element, nullContext()), observer::onError);
  }

  @Override
  public ObserveHandle observeBulk(
      String name, Position position, boolean stopAtCurrent, LogObserver observer) {

    checkSupportedPosition(position);
    return observe(element -> observer.onNext(element, nullContext()), observer::onError);
  }

  @Override
  public ObserveHandle observeBulkPartitions(
      String name,
      Collection<Partition> partitions,
      Position position,
      boolean stopAtCurrent,
      LogObserver observer) {

    return observeBulk(name, position, observer);
  }

  @Override
  public ObserveHandle observeBulkOffsets(Collection<Offset> offsets, LogObserver observer) {

    return observeBulk(null, Position.NEWEST, observer);
  }

  @Override
  public Factory<?> asFactory() {
    final EntityDescriptor entity = getEntityDescriptor();
    final URI uri = getUri();
    final Map<String, Object> cfg = this.cfg;
    return repo -> new WebsocketReader(entity, uri, cfg);
  }

  private OnNextContext nullContext() {
    return asOnNextContext((succ, err) -> {}, OFFSET);
  }

  private void checkSupportedPosition(Position position) {
    if (position == Position.OLDEST) {
      throw new UnsupportedOperationException("Cannot read OLDEST data from websocket");
    }
  }
}
