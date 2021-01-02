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

import com.typesafe.config.Config;
import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import java.net.URI;
import java.util.Map;
import lombok.AccessLevel;
import lombok.Getter;

/** A {@link DataAccessorFactory} for PubSub. */
@Stable
public class PubSubStorage implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  public static final String CFG_DEFAULT_MAX_ACK_DEADLINE = "pubsub.default.deadline-max-ms";
  public static final String CFG_DEFAULT_SUBSCRIPTION_AUTOCREATE =
      "pubsub.default.subscription.auto-create";
  public static final String CFG_DEFAULT_SUBSCRIPTION_ACK_DEADLINE =
      "pubsub.default.subscription.ack-deadline";
  public static final String CFG_DEFAULT_WATERMARK_ESTIMATE_DURATION =
      "pubsub.default.watermark.estimate-duration";
  public static final String CFG_DEFAULT_ALLOWED_TIMESTAMP_SKEW =
      "pubsub.default.watermark.allowed-timestamp-skew";

  @Getter(AccessLevel.PACKAGE)
  private long defaultMaxAckDeadlineMs = 600000;

  @Getter(AccessLevel.PACKAGE)
  private boolean defaultSubscriptionAutoCreate = true;

  @Getter(AccessLevel.PACKAGE)
  private int defaultSubscriptionAckDeadlineSeconds = 600;

  @Getter(AccessLevel.PACKAGE)
  private Integer defaultWatermarkEstimateDuration = null;

  @Getter(AccessLevel.PACKAGE)
  private long defaultAllowedTimestampSkew = 200L;

  @Override
  public void setup(Repository repo) {
    if (repo instanceof ConfigRepository) {
      Config cfg = ((ConfigRepository) repo).getConfig();
      if (cfg.hasPath(CFG_DEFAULT_MAX_ACK_DEADLINE)) {
        defaultMaxAckDeadlineMs = cfg.getInt(CFG_DEFAULT_MAX_ACK_DEADLINE);
      }
      if (cfg.hasPath(CFG_DEFAULT_SUBSCRIPTION_AUTOCREATE)) {
        defaultSubscriptionAutoCreate = cfg.getBoolean(CFG_DEFAULT_SUBSCRIPTION_AUTOCREATE);
      }
      if (cfg.hasPath(CFG_DEFAULT_SUBSCRIPTION_ACK_DEADLINE)) {
        defaultSubscriptionAckDeadlineSeconds = cfg.getInt(CFG_DEFAULT_SUBSCRIPTION_ACK_DEADLINE);
      }
      if (cfg.hasPath(CFG_DEFAULT_WATERMARK_ESTIMATE_DURATION)) {
        defaultWatermarkEstimateDuration = cfg.getInt(CFG_DEFAULT_WATERMARK_ESTIMATE_DURATION);
      }
      if (cfg.hasPath(CFG_DEFAULT_ALLOWED_TIMESTAMP_SKEW)) {
        defaultAllowedTimestampSkew = cfg.getLong(CFG_DEFAULT_ALLOWED_TIMESTAMP_SKEW);
      }
    }
  }

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator direct, EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

    return new PubSubAccessor(this, entityDesc, uri, cfg);
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("gps") ? Accept.ACCEPT : Accept.REJECT;
  }
}
