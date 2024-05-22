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

import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.annotations.Stable;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.io.pubsub.util.PubSubUtils;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** A {@link CommitLogReader} for Google PubSub. */
@Stable
@Slf4j
class PubSubReader extends AbstractPubSubReader implements CommitLogReader {

  PubSubReader(PubSubAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getUri(), accessor, context);
  }

  @Override
  protected List<StreamElement> parseElements(PubsubMessage m) {
    return PubSubUtils.toStreamElement(
            getEntityDescriptor(), m.getMessageId(), m.getData().toByteArray())
        .stream()
        .collect(Collectors.toList());
  }

  public Factory<?> asFactory() {
    final PubSubAccessor accessor = this.accessor;
    final Context context = this.context;
    return repo -> new PubSubReader(accessor, context);
  }
}
