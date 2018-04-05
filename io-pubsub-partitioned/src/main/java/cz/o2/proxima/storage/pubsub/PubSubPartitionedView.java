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
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AbstractStorage;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.commitlog.ObserveHandle;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.util.Pair;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

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
  private final CommitLogReader reader;

  PubSubPartitionedView(PartitionedPubSubAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getURI());
    this.projectId = accessor.getProject();
    this.topic = accessor.getTopic();
    this.partitioner = accessor.getPartitioner();
    this.options = accessor.getOptions();
    this.numPartitions = accessor.getNumPartitions();
    this.reader = accessor.getCommitLogReader(context).orElseThrow(
        () -> new IllegalStateException("Fix code! Need a commit log reader!"));
  }

  @Override
  public List<Partition> getPartitions() {
    return IntStream.range(0, numPartitions)
        .mapToObj(i -> (Partition) () -> i)
        .collect(Collectors.toList());
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
      PCollection<StreamElement> msgs = pubsubIO(bf, getEntityDescriptor(), reader);
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
      PCollection<StreamElement> msgs = pubsubIO(
          bf, projectId, topic,
          name, getEntityDescriptor(), reader);

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
      BeamFlow flow, EntityDescriptor entity, CommitLogReader reader) {

    return pubsubIO(flow, projectId, topic, null, entity, reader);
  }

  @VisibleForTesting
  PCollection<StreamElement> pubsubIO(
      BeamFlow flow, String projectId, String topic,
      @Nullable String subscription, EntityDescriptor entity,
      CommitLogReader reader) {

    final Pipeline pipeline = flow.getPipeline();
    PCollection<Integer> parts = pipeline.apply(
        Create.of(IntStream.range(0, numPartitions)
            .mapToObj(Integer::valueOf)
            .collect(Collectors.toList())));

    return parts
        .apply(ParDo.of(readParDo(reader, subscription)))
        .setCoder(new KryoCoder<>());
  }

  @DoFn.UnboundedPerElement
  private static class ReadDoFn extends DoFn<Integer, StreamElement> {

    private class BulkRestrictionTracker implements RestrictionTracker<UnsplittableRestriction> {

      private final UnsplittableRestriction restriction;
      private BulkLogObserver.OffsetCommitter lastPolled;

      BulkRestrictionTracker(UnsplittableRestriction restriction) {
        this.restriction = restriction;
      }

      @Override
      public UnsplittableRestriction currentRestriction() {
        return restriction;
      }

      @Override
      public UnsplittableRestriction checkpoint() {
        if (lastPolled != null) {
          System.err.println(" *** confirm " + lastPolled);
          lastPolled.confirm();
        }
        restriction.setStop(true);
        System.err.println(" *** checkpoint ");
        return new UnsplittableRestriction(false);
      }

      @Override
      public void checkDone() throws IllegalStateException {

      }

      @Override
      public String toString() {
        return "BulkRestrictionTracker(" + restriction + ")";
      }

      boolean isStop() {
        if (restriction != null) {
          return restriction.isStop();
        }
        return true;
      }

    }

    private class UnsplittableRestriction {

      @Getter
      @Setter
      private boolean stop = false;

      UnsplittableRestriction(boolean stop) {
        this.stop = stop;
      }

      UnsplittableRestriction() {
        handle = reader.observeBulk(
            subscription,
            Position.NEWEST,
            new BulkLogObserver() {

              @Override
              public boolean onNext(
                  StreamElement ingest,
                  BulkLogObserver.OffsetCommitter committer) {

                try {
                  queue.put(Pair.of(committer, ingest));
                } catch (InterruptedException ex) {
                  throw new RuntimeException(ex);
                }
                return true;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }

            });
      }

    }

    private final CommitLogReader reader;
    private final @Nullable String subscription;
    private final BlockingQueue<Pair<BulkLogObserver.OffsetCommitter, StreamElement>> queue;
    private ObserveHandle handle;
    long watermark = -1L;

    private ReadDoFn(
        CommitLogReader reader,
        String subscription) {

      this.reader = reader;
      this.subscription = subscription;
      this.queue = new ArrayBlockingQueue<>(100);
    }

    @Setup
    public void setup() {
    }

    @Teardown
    public void tearDown() {
      handle.cancel();
    }

    @ProcessElement
    public ProcessContinuation process(
        ProcessContext context, BulkRestrictionTracker tracker)
        throws InterruptedException {

      System.err.println(" **** " + tracker);
      while (!tracker.isStop()) {
        Pair<BulkLogObserver.OffsetCommitter, StreamElement> poll;
        poll = queue.poll(100, TimeUnit.MILLISECONDS);
        if (poll != null) {
          tracker.lastPolled = poll.getFirst();
          StreamElement elem = poll.getSecond();
          Instant instant = new Instant(elem.getStamp());
          context.outputWithTimestamp(elem, instant);
          if (watermark < elem.getStamp()) {
            context.updateWatermark(instant);
          }
        }
      }
      System.err.println(" *** stop");
      return ProcessContinuation.stop();
    }

    @GetInitialRestriction
    public UnsplittableRestriction initialRestriction(Integer partition) {
      System.err.println(" *** initialRestriction " + partition + " in " + this);
      return new UnsplittableRestriction();
    }

    @NewTracker
    public BulkRestrictionTracker newTracker(UnsplittableRestriction restriction) {
      System.err.println(" *** newTracker " + restriction);
      return new BulkRestrictionTracker(restriction);
    }

    @GetRestrictionCoder
    public Coder<UnsplittableRestriction> getRestrictionCoder() {
      return new KryoCoder<>();
    }

  }

  private static DoFn<Integer, StreamElement> readParDo(
      CommitLogReader reader, @Nullable String subscription) {

    return new ReadDoFn(reader, subscription);
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

}
