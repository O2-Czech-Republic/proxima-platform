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
package cz.o2.proxima.direct.pubsub;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.UriUtil;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

/** A {@link DataAccessor} for Google PubSub. */
class PubSubAccessor extends AbstractStorage implements DataAccessor {

  private static final long serialVersionUID = 1L;

  public static final String CFG_MAX_ACK_DEADLINE = "pubsub.deadline-max-ms";
  public static final String CFG_SUBSCRIPTION_AUTOCREATE = "pubsub.subscription.auto-create";
  public static final String CFG_SUBSCRIPTION_ACK_DEADLINE = "pubsub.subscription.ack-deadline";

  @Getter private final Map<String, Object> cfg;

  @Getter private final String project;

  @Getter private final String topic;

  @Getter private final int maxAckDeadline;

  @Getter private final int subscriptionAckDeadline;

  @Getter private final boolean subscriptionAutoCreate;

  @Getter private final PubSubWatermarkConfiguration watermarkConfiguration;

  PubSubAccessor(PubSubStorage storage, EntityDescriptor entity, URI uri, Map<String, Object> cfg) {

    super(entity, uri);
    this.cfg = cfg;
    project = uri.getAuthority();
    topic = UriUtil.getPathNormalized(uri);
    maxAckDeadline =
        Optional.ofNullable(cfg.get(CFG_MAX_ACK_DEADLINE))
            .map(Object::toString)
            .map(Integer::valueOf)
            .orElse((int) storage.getDefaultMaxAckDeadlineMs());
    subscriptionAutoCreate =
        Optional.ofNullable(cfg.get(CFG_SUBSCRIPTION_AUTOCREATE))
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(storage.isDefaultSubscriptionAutoCreate());
    subscriptionAckDeadline =
        Optional.ofNullable(cfg.get(CFG_SUBSCRIPTION_ACK_DEADLINE))
            .map(Object::toString)
            .map(Integer::valueOf)
            .orElse(storage.getDefaultSubscriptionAckDeadlineSeconds());

    long defaultEstimateDuration =
        storage.getDefaultWatermarkEstimateDuration() == null
            ? subscriptionAckDeadline * 1000
            : storage.getDefaultWatermarkEstimateDuration();

    watermarkConfiguration =
        new PubSubWatermarkConfiguration(
            cfg, defaultEstimateDuration, storage.getDefaultAllowedTimestampSkew());

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
