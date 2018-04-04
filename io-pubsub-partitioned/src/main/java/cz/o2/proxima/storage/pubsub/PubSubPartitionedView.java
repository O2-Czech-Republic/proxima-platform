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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.pubsub.proto.PubSub;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.beam.BeamFlow;
import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link PartitionedView} for Google PubSub.
 */
@Slf4j
class PubSubPartitionedView extends AbstractStorage implements PartitionedView {

  private final transient PipelineOptions options;
  private final String projectId;
  private final String topic;
  private final Partitioner partitioner;
  private final int numPartitions;

  PubSubPartitionedView(PartitionedPubSubAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getURI());
    this.projectId = accessor.getProjectId();
    this.topic = accessor.getTopic();
    this.partitioner = accessor.getPartitioner();
    this.options = accessor.getOptions();
    this.numPartitions = accessor.getNumPartitions();
  }

  @Override
  public List<Partition> getPartitions() {
    return Lists.newArrayList(() -> 0);
  }

  @Override
  public <T> Dataset<T> observePartitions(
      Collection<Partition> partitions,
      PartitionedLogObserver<T> observer) {

    return observePartitions(BeamFlow.create(
        Pipeline.create(options)), partitions, observer);
  }

  @Override
  public <T> Dataset<T> observe(String name, PartitionedLogObserver<T> observer) {
    return observe(
        BeamFlow.create(Pipeline.create(options)),
        name, observer);
  }

  @Override
  public <T> Dataset<T> observePartitions(
      Flow flow,
      Collection<Partition> partitions,
      PartitionedLogObserver<T> observer) {

    if (flow instanceof BeamFlow) {
      BeamFlow bf = (BeamFlow) flow;
      final Pipeline pipeline;
      try {
        pipeline = bf.getPipeline();
      } catch (NullPointerException ex) {
        throw new IllegalStateException(
            "Please create flow with BeamFlow.create(pipeline)", ex);
      }
      PCollection<StreamElement> msgs = pubsubIO(pipeline, getEntityDescriptor());
      return applyObserver(
          msgs, getEntityDescriptor(), partitioner,
          numPartitions, observer, bf);
    }
    throw new UnsupportedOperationException(
        "This view cooperates only with BeamFlow. Use euphoria-beam as executor "
      + "and construct flow by BeamFlow#create");
  }

  @Override
  public <T> Dataset<T> observe(
      Flow flow, String name,
      PartitionedLogObserver<T> observer) {

    if (flow instanceof BeamFlow) {
      BeamFlow bf = (BeamFlow) flow;
      final Pipeline pipeline;
      try {
        pipeline = bf.getPipeline();
      } catch (NullPointerException ex) {
        throw new IllegalStateException(
            "Please create flow with BeamFlow.create(pipeline)", ex);
      }
      PCollection<StreamElement> msgs = pubsubIO(
          pipeline, projectId, topic,
          name, getEntityDescriptor());

      return applyObserver(
          msgs, getEntityDescriptor(), partitioner,
          numPartitions, observer, bf);
    }
    throw new UnsupportedOperationException(
        "This view cooperates only with BeamFlow. Use euphoria-beam as executor "
      + "and construct flow by BeamFlow#create");

  }

  private static <T> Dataset<T> applyObserver(
      PCollection<StreamElement> msgs,
      EntityDescriptor entity,
      Partitioner partitioner,
      int numPartitions,
      PartitionedLogObserver<T> observer,
      BeamFlow flow) {

    PCollection<KV<Integer, StreamElement>> parts = msgs.apply(
        MapElements
            .into(new TypeDescriptor<KV<Integer, StreamElement>>() { })
            .via(e -> KV.of((partitioner.getPartition(e)
                & Integer.MAX_VALUE) % numPartitions, e)))
        .setCoder(KvCoder.of(VarIntCoder.of(), new KryoCoder<>()));
    PCollectionList<KV<Integer, StreamElement>> partitioned = parts.apply(
        org.apache.beam.sdk.transforms.Partition.of(
            numPartitions, (message, partitionCount) -> {
              return (int) message.getKey();
            }));

    List<PCollection<T>> flatten = new ArrayList<>();
    for (int i = 0; i < partitioned.size(); i++) {
      final int partitionId = i;
      flatten.add(
          partitioned.get(partitionId)
              .apply(ParDo.of(toDoFn(observer, partitionId)))
              .setCoder(new KryoCoder<>()));
    }
    PCollectionList<T> l = PCollectionList.of(flatten);
    PCollection<T> result = l
        .apply(Flatten.pCollections())
        .setCoder(new KryoCoder<>());
    return flow.wrapped(result);
  }

  private PCollection<StreamElement> pubsubIO(
      Pipeline pipeline, EntityDescriptor entity) {

    return pubsubIO(pipeline, projectId, topic, null, entity);
  }

  @VisibleForTesting
  PCollection<StreamElement> pubsubIO(
      Pipeline pipeline, String projectId, String topic,
      @Nullable String subscription, EntityDescriptor entity) {

    final PCollection<PubsubMessage> msgs;
    if (subscription != null) {
      msgs = pipeline.apply(PubsubIO.readMessages().fromSubscription(
          String.format("projects/%s/subscriptions/%s", projectId, subscription)));
    } else {
      msgs = pipeline.apply(PubsubIO.readMessages()
        .fromTopic(String.format("projects/%s/topics/%s", projectId, topic)));
    }
    return msgs.apply(ParDo.of(
        new DoFn<PubsubMessage, StreamElement>() {

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @ProcessElement
      public void process(ProcessContext context) {
        toElement(entity, context.element())
            .ifPresent(context::output);
      }

    })).setCoder(new KryoCoder<>());
  }

  private static <T> DoFn<KV<Integer, StreamElement>, T> toDoFn(
      PartitionedLogObserver<T> observer, int partitionId) {

    return new DoFn<KV<Integer, StreamElement>, T>() {

      boolean initialized = false;

      @StateId("seq")
      private final StateSpec<ValueState<Void>> seq = StateSpecs.value();

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @Setup
      public void setup() {
        if (!initialized) {
          observer.onRepartition(Arrays.asList(() -> partitionId));
          initialized = true;
        }
      }

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @Teardown
      public void tearDown() {
        observer.onCompleted();
      }

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @ProcessElement
      public void process(
          ProcessContext context,
          @StateId("seq") ValueState<Void> ign) {

        KV<Integer, StreamElement> tmp = context.element();
        StreamElement elem = tmp.getValue();
        try {
          boolean cont = observer.onNext(elem, (succ, exc) -> {
            if (!succ) {
              throw new RuntimeException(exc);
            }
          }, () -> partitionId, context::output);
          if (!cont) {
            // FIXME: how to interrupt the processing?
          }
        } catch (Throwable err) {
          observer.onError(err);
        }
      }
    };
  }

  private static Optional<StreamElement> toElement(
      EntityDescriptor entity, PubsubMessage message) {

    try {
      PubSub.KeyValue parsed = PubSub.KeyValue.parseFrom(message.getPayload());
      // FIXME: need to get access to messageId and publish time
      // from the Beam's PubsubMessage wrapper
      // fallback to uuid regeneration and ingestion time for now
      // that is not 100% correct, but good enough for now
      String uuid = UUID.randomUUID().toString();
      long stamp = System.currentTimeMillis();
      Optional<AttributeDescriptor<Object>> attr = entity.findAttribute(parsed.getAttribute());
      if (attr.isPresent()) {
        if (parsed.getDeleteWildcard()) {
          return Optional.of(StreamElement.deleteWildcard(
              entity, attr.get(), uuid, parsed.getKey(), stamp));
        } else if (parsed.getDelete()) {
          return Optional.of(StreamElement.delete(
              entity, attr.get(), uuid, parsed.getKey(),
              parsed.getAttribute(), stamp));
        }
        return Optional.of(StreamElement.update(
            entity, attr.get(), uuid, parsed.getKey(), parsed.getAttribute(),
            stamp, parsed.getValue().toByteArray()));
      }
      log.warn("Missing attribute {} of entity {}", parsed.getAttribute(), entity);
    } catch (InvalidProtocolBufferException ex) {
      log.error("Failed to parse input {}", message, ex);
    }
    return Optional.empty();
  }

}
