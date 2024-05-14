/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.pubsub;

import cz.o2.proxima.core.annotations.Stable;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.io.pubsub.proto.PubSub.KeyValue;
import cz.o2.proxima.io.pubsub.util.PubSubUtils;
import lombok.extern.slf4j.Slf4j;

/** A {@link OnlineAttributeWriter} for Google PubSub. */
@Stable
@Slf4j
class PubSubWriter extends AbstractPubSubWriter implements OnlineAttributeWriter {

  PubSubWriter(PubSubAccessor accessor, Context context) {
    super(accessor, context);
  }

  @Override
  public synchronized void write(StreamElement data, CommitCallback statusCallback) {
    log.debug("Writing data {} to {}", data, accessor.getUri());
    KeyValue kv = PubSubUtils.toKeyValue(data);
    writeMessage(
        data.getUuid(),
        data.getStamp(),
        kv,
        (succ, exc) -> {
          if (!succ) {
            log.warn("Failed to publish {} to pubsub", data, exc);
          } else {
            log.debug("Committing processing of {} with success", data);
          }
          statusCallback.commit(succ, exc);
        });
  }

  @Override
  public OnlineAttributeWriter.Factory<?> asFactory() {
    final PubSubAccessor accessor = this.accessor;
    final Context context = this.context;
    return repo -> new PubSubWriter(accessor, context);
  }
}
