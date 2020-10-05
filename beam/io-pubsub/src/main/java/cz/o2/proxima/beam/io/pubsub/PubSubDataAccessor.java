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
package cz.o2.proxima.beam.io.pubsub;

import com.google.common.base.Preconditions;
import cz.o2.proxima.beam.core.DataAccessor;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.pubsub.proto.PubSub;
import cz.o2.proxima.pubsub.shaded.com.google.protobuf.InvalidProtocolBufferException;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.UriUtil;
import cz.o2.proxima.storage.commitlog.Position;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;

/** A {@link DataAccessor} for PubSub. */
@Slf4j
public class PubSubDataAccessor implements DataAccessor {

  private static final long serialVersionUID = 1L;

  private final RepositoryFactory repoFactory;
  private final EntityDescriptor entity;
  @Getter private final URI uri;
  private final String topic;

  PubSubDataAccessor(Repository repo, EntityDescriptor entity, URI uri) {
    this.repoFactory = repo.asFactory();
    this.entity = entity;
    this.uri = uri;
    String project = uri.getAuthority();
    String topic = UriUtil.getPathNormalized(uri);
    Preconditions.checkArgument(!project.isEmpty(), "Authority in URI %s must not be empty", uri);
    Preconditions.checkArgument(!topic.isEmpty(), "Path in URI %s must specify topic", uri);
    this.topic = String.format("projects/%s/topics/%s", project, topic);
  }

  static Optional<StreamElement> toElement(EntityDescriptor entity, byte[] payload) {

    try {
      String uuid = UUID.randomUUID().toString();
      PubSub.KeyValue parsed = PubSub.KeyValue.parseFrom(payload);
      long stamp = parsed.getStamp();
      Optional<AttributeDescriptor<Object>> attribute =
          entity.findAttribute(parsed.getAttribute(), true /* allow protected */);
      if (attribute.isPresent()) {
        if (parsed.getDelete()) {
          return Optional.of(
              StreamElement.delete(
                  entity, attribute.get(), uuid, parsed.getKey(), parsed.getAttribute(), stamp));
        } else if (parsed.getDeleteWildcard()) {
          return Optional.of(
              StreamElement.deleteWildcard(
                  entity, attribute.get(), uuid, parsed.getKey(), parsed.getAttribute(), stamp));
        }
        return Optional.of(
            StreamElement.upsert(
                entity,
                attribute.get(),
                uuid,
                parsed.getKey(),
                parsed.getAttribute(),
                stamp,
                parsed.getValue().toByteArray()));
      }
      log.warn("Failed to find attribute {} in entity {}", parsed.getAttribute(), entity);
    } catch (InvalidProtocolBufferException ex) {
      log.warn("Failed to parse message {}", Arrays.toString(payload), ex);
    }
    return Optional.empty();
  }

  @Override
  public PCollection<StreamElement> createStream(
      String name,
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      long limit) {

    PCollection<PubsubMessage> input = pipeline.apply(PubsubIO.readMessages().fromTopic(topic));
    PCollection<StreamElement> parsed =
        FlatMap.of(input)
            .using(
                (PubsubMessage in, Collector<StreamElement> ctx) ->
                    toElement(entity, in.getPayload()).ifPresent(ctx::collect),
                TypeDescriptor.of(StreamElement.class))
            .output()
            .setCoder(StreamElementCoder.of(repoFactory));
    if (eventTime) {
      return AssignEventTime.of(parsed)
          .using(StreamElement::getStamp, Duration.millis(Long.MAX_VALUE))
          .output()
          .setCoder(parsed.getCoder());
    }
    return parsed;
  }

  @Override
  public PCollection<StreamElement> createBatch(
      Pipeline pipeline, List<AttributeDescriptor<?>> attrs, long startStamp, long endStamp) {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public PCollection<StreamElement> createStreamFromUpdates(
      Pipeline pipeline,
      List<AttributeDescriptor<?>> attributes,
      long startStamp,
      long endStamp,
      long limit) {

    throw new UnsupportedOperationException("Not supported yet.");
  }
}
