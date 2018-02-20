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

import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.pubsub.proto.PubSub;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.o2.proxima.view.PartitionedView;
import cz.seznam.euphoria.beam.BeamFlow;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link PartitionedView} for Google PubSub.
 */
class PubSubPartitionedView extends AbstractStorage implements PartitionedView {

  private final PipelineOptions options;
  private final String topic;
  private final Partitioner partitioner;
  private final int numPartitions;

  PubSubPartitionedView(PartitionedPubSubAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getURI());
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
      PCollection<PubsubMessage> msgs = pipeline.apply(
          PubsubIO.readMessages()
              .fromTopic(topic));
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
      PCollection<PubsubMessage> msgs = pipeline.apply(
          PubsubIO.readMessages()
              .fromTopic(topic));
      return applyObserver(
          msgs, getEntityDescriptor(), partitioner,
          numPartitions, observer, bf);
    }
    throw new UnsupportedOperationException(
        "This view cooperates only with BeamFlow. Use euphoria-beam as executor "
      + "and construct flow by BeamFlow#create");

  }

  private static <T> Dataset<T> applyObserver(
      PCollection<PubsubMessage> msgs,
      EntityDescriptor entity,
      Partitioner partitioner,
      int numPartitions,
      PartitionedLogObserver<T> observer,
      BeamFlow flow) {

    PCollection<StreamElement> parsed = msgs.apply(MapElements
        .into(TypeDescriptor.of(StreamElement.class))
        .via(m -> toElement(entity, m)));
    PCollectionList<StreamElement> partitioned = parsed.apply(
        org.apache.beam.sdk.transforms.Partition.of(
            numPartitions, (message, partitionCount) -> {
              return (partitioner.getPartition(message)
                  & Integer.MAX_VALUE) % partitionCount;
            }));

    List<PCollection<T>> flatten = new ArrayList<>();
    for (int i = 0; i < partitioned.size(); i++) {
      final int partitionId = i;
      flatten.add(partitioned.get(partitionId)
          .apply(ParDo.of(toDoFn(observer, partitionId))));
    }
    PCollectionList<T> l = PCollectionList.of(flatten);
    PCollection<T> result = l.apply(Flatten.pCollections());
    return flow.wrapped(result);
  }


  private static <T> DoFn<StreamElement, T> toDoFn(
      PartitionedLogObserver<T> observer, int partitionId) {

    return new DoFn<StreamElement, T>() {

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @Setup
      public void setup() {
        observer.onRepartition(Arrays.asList(() -> partitionId));
      }

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @Teardown
      public void tearDown() {
        observer.onCompleted();
      }

      @SuppressFBWarnings("UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS")
      @ProcessElement
      public void process(ProcessContext context) {
        StreamElement elem = context.element();
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

  private static StreamElement toElement(
      EntityDescriptor entity, PubsubMessage message) {

    try {
      PubSub.KeyValue parsed = PubSub.KeyValue.parseFrom(message.getPayload());
      if (parsed.getDeleteWildcard()) {

      } else if (parsed.getDelete()) {

      }
      throw new UnsupportedOperationException();
    } catch (InvalidProtocolBufferException ex) {
      throw new RuntimeException(ex);
    }
  }

}
