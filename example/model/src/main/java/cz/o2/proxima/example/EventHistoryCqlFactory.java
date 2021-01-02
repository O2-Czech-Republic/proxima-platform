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
package cz.o2.proxima.example;

import cz.o2.proxima.direct.cassandra.TransformingCqlFactory;
import cz.o2.proxima.example.event.Event.BaseEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;

/**
 * Producent of CQL updates to casssandra for gateway. The datamodel stored in cassandra looks like
 * this:
 *
 * <pre>
 *   CREATE TABLE proxima.gateway (
 *     hgw varchar PRIMARY KEY,
 *     armed blob,
 *     geo_users blob,
 *     wifi_devices blob,
 *     status blob
 *   )
 * </pre>
 */
public class EventHistoryCqlFactory extends TransformingCqlFactory<BaseEvent> {
  public EventHistoryCqlFactory() {
    super(
        ingest -> {
          try {
            return BaseEvent.parseFrom(ingest.getValue());
          } catch (IOException ex) {
            throw new IllegalArgumentException(ex);
          }
        },
        Arrays.asList("user" /* primary key */, "stamp" /* secondary key */, "event" /* payload */),
        Arrays.asList(
            p -> p.getSecond().getUserName(),
            p -> new Date(p.getSecond().getStamp()),
            p -> ByteBuffer.wrap(p.getSecond().toByteArray())),
        e -> !e.getUserName().isEmpty());
  }
}
