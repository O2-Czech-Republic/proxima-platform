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

import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.AbstractStorage.SerializableAbstractStorage;
import cz.o2.proxima.core.storage.UriUtil;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.internal.com.google.common.base.Strings;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;

/** A {@link DataAccessor} for Google PubSub. */
public class PubSubAccessor extends SerializableAbstractStorage implements DataAccessor {

  @Getter
  @Value
  static class BulkConfig implements Serializable {
    int bulkSize;
    int flushMs;
    boolean deflate;
  }

  private static final long serialVersionUID = 2L;

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

  @Getter private final @Nullable BulkConfig bulk;

  PubSubAccessor(PubSubStorage storage, EntityDescriptor entity, URI uri, Map<String, Object> cfg) {
    super(entity, uri);
    this.cfg = cfg;
    project = uri.getAuthority();
    topic = UriUtil.getPathNormalized(uri);
    this.bulk = parseBulkConfig(uri);
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
            ? subscriptionAckDeadline * 1000L
            : storage.getDefaultWatermarkEstimateDuration();

    watermarkConfiguration =
        new PubSubWatermarkConfiguration(
            cfg, defaultEstimateDuration, storage.getDefaultAllowedTimestampSkew());

    Preconditions.checkArgument(!Strings.isNullOrEmpty(project), "Authority cannot be empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "Path has to represent topic");
  }

  private BulkConfig parseBulkConfig(URI uri) {
    boolean isBulk = uri.getScheme().equals("gps-bulk");
    if (isBulk) {
      Map<String, String> parsed = UriUtil.parseQuery(uri);
      int bulksize = Optional.ofNullable(parsed.get("bulk")).map(Integer::valueOf).orElse(100);
      int flush = Optional.ofNullable(parsed.get("flush")).map(Integer::valueOf).orElse(100);
      boolean deflate =
          Optional.ofNullable(parsed.get("deflate")).map(Boolean::valueOf).orElse(false);
      return new BulkConfig(bulksize, flush, deflate);
    }
    return null;
  }

  public boolean isBulk() {
    return bulk != null;
  }

  @Override
  public Optional<CommitLogReader> getCommitLogReader(Context context) {
    if (isBulk()) {
      return Optional.of(new PubSubBulkReader(this, context));
    }
    return Optional.of(new PubSubReader(this, context));
  }

  @Override
  public Optional<AttributeWriterBase> getWriter(Context context) {
    if (isBulk()) {
      return Optional.of(new PubSubBulkWriter(this, context));
    }
    return Optional.of(new PubSubWriter(this, context));
  }
}
