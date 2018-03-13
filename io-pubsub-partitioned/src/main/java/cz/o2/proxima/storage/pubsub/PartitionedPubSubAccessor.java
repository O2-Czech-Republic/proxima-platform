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

import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.URIUtil;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import cz.seznam.euphoria.shadow.com.google.common.base.Strings;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * {@link DataAccessor} for partitioned pubsub view.
 */
@Slf4j
class PartitionedPubSubAccessor extends AbstractStorage implements DataAccessor {

  public static final String CFG_PARTITIONER = "partitioner";
  public static final String CFG_RUNNER = "runner";
  public static final String CFG_NUM_PARTITIONS = "num-partitions";

  @Getter
  private final String topic;

  @Getter
  private final Partitioner partitioner;

  @Getter
  private final int numPartitions;

  @Getter
  private final PipelineOptions options;

  PartitionedPubSubAccessor(
      EntityDescriptor entity,
      URI uri, Map<String, Object> cfg) {

    super(entity, uri);
    topic = URIUtil.getPathNormalized(uri);
    partitioner = Optional.ofNullable(cfg.get(CFG_PARTITIONER))
        .map(Object::toString)
        .map(c -> Classpath.findClass(c, Partitioner.class))
        .map(Classpath::newInstance)
        .orElseThrow(() -> new IllegalArgumentException(
            "Missing " + CFG_PARTITIONER + " as partitioner class"));
    options = asOptions(cfg);
    this.numPartitions = Optional.ofNullable(cfg.get(CFG_NUM_PARTITIONS))
        .map(Object::toString)
        .map(Integer::valueOf)
        .orElse(1);

    Preconditions.checkArgument(!Strings.isNullOrEmpty(topic), "Path has to represent topic");
  }

  @Override
  public Optional<PartitionedView> getPartitionedView(Context context) {
    return Optional.of(new PubSubPartitionedView(this, context));
  }

  @SuppressWarnings("unchecked")
  private PipelineOptions asOptions(Map<String, Object> cfg) {
    PipelineOptions ret = PipelineOptionsFactory.create();
    ret.setRunner(Optional.ofNullable(cfg.get(CFG_RUNNER))
        .map(Object::toString)
        .map(c -> Classpath.findClass(c, PipelineRunner.class))
        .orElse((Class) DirectRunner.class));
    return ret;
  }

}
