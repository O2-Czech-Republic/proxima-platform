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
package cz.o2.proxima.storage.pubsub;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.URIUtil;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

/**
 * A {@link DataAccessor} for Google PubSub.
 */
class PubSubAccessor extends AbstractStorage implements DataAccessor {

  public static final String CFG_MAX_ACK_DEADLINE = "pubsub.deadline-max-ms";
  public static final String CFG_SUBSCRIPTION_AUTOCREATE = "pubsub.subscription.auto-create";
  public static final String CFG_SUBSCRIPTION_ACK_DEADLINE = "pubsub.subscription.ack-deadline";

  @Getter
  private final String project;

  @Getter
  private final String topic;

  @Getter
  private final int maxAckDeadline;

  @Getter
  private final int subscriptionAckDeadline;

  @Getter
  private final boolean subscriptionAutoCreate;

  PubSubAccessor(EntityDescriptor entity, URI uri, Map<String, Object> cfg) {
    super(entity, uri);
    project = uri.getAuthority();
    topic = URIUtil.getPathNormalized(uri);
    maxAckDeadline = Optional.ofNullable(cfg.get(CFG_MAX_ACK_DEADLINE))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(60000);
    subscriptionAutoCreate = Optional.ofNullable(cfg.get(CFG_SUBSCRIPTION_AUTOCREATE))
        .map(Object::toString)
        .map(Boolean::valueOf)
        .orElse(false);
    subscriptionAckDeadline = Optional.ofNullable(cfg.get(CFG_SUBSCRIPTION_ACK_DEADLINE))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(10);

    Preconditions.checkArgument(!Strings.isNullOrEmpty(project), "Authority cannot be empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "Path has to represent topic");
  }

  @Override
  public Optional<CommitLogReader> getCommitLogReader(Context context) {
    return Optional.of(new PubSubReader(this, context));
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    return Optional.of(new PubSubWriter(this, context));
  }

}
