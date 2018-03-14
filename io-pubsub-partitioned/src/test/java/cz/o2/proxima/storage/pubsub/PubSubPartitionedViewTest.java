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

import com.esotericsoftware.kryo.Kryo;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeDescriptorBase;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import static cz.o2.proxima.storage.pubsub.PartitionedPubSubAccessor.CFG_NUM_PARTITIONS;
import static cz.o2.proxima.storage.pubsub.PartitionedPubSubAccessor.CFG_PARTITIONER;
import cz.o2.proxima.storage.pubsub.proto.ProtobufKryo;
import cz.o2.proxima.storage.pubsub.proto.PubSub.KeyValue;
import cz.o2.proxima.util.Pair;
import cz.o2.proxima.view.PartitionedLogObserver;
import cz.seznam.euphoria.beam.BeamFlow;
import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.functional.VoidFunction;
import cz.seznam.euphoria.core.client.operator.ReduceWindow;
import cz.seznam.euphoria.core.client.util.Sums;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ArrayListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.guava.LinkedHashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.LinkedListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ReverseListSerializer;
import de.javakaffee.kryoserializers.guava.TreeMultimapSerializer;
import de.javakaffee.kryoserializers.guava.UnmodifiableNavigableSetSerializer;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test suite for {@link PubSubPartitionedView}.
 */
public class PubSubPartitionedViewTest implements Serializable {

  private static class TestedPubSubPartitionedView extends PubSubPartitionedView {

    final List<PubsubMessage> input;

    public TestedPubSubPartitionedView(
        PartitionedPubSubAccessor accessor, Context context,
        List<PubsubMessage> input) {

      super(accessor, context);
      this.input = input;
    }

    @Override
    PTransform<PBegin, PCollection<PubsubMessage>> pubsubIO(
        String projectId, String topic, @Nullable String subscription) {

      return Create.of(input);
    }

  }

  public static class FirstBytePartitioner implements Partitioner {

    @Override
    public int getPartition(StreamElement element) {
      if (element.isDelete()) {
        return 0;
      }
      return element.getValue()[0];
    }

  }

  private final transient Repository repo = Repository.of(ConfigFactory.empty());
  private final AttributeDescriptorBase<byte[]> attr = AttributeDescriptor.newBuilder(repo)
      .setEntity("entity")
      .setName("attr")
      .setSchemeURI(new URI("bytes:///"))
      .build();

  private final EntityDescriptor entity = EntityDescriptor.newBuilder()
      .setName("entity")
      .addAttribute(attr)
      .build();

  public PubSubPartitionedViewTest() throws Exception {

  }

  PubSubPartitionedView createView(
      Class<? extends Partitioner> partitionerClass,
      int numPartitions,
      List<PubsubMessage> messages) {

    try {
      return new TestedPubSubPartitionedView(
          new PartitionedPubSubAccessor(
              entity, new URI("pgps://dummy/dummy"),
              Stream
                  .of(
                      Pair.of(CFG_PARTITIONER, partitionerClass.getName()),
                      Pair.of(CFG_NUM_PARTITIONS, String.valueOf(numPartitions)))
                  .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond))),
          context(), messages);
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void testViewConsumption() {
    PubSubPartitionedView view = createView(
        FirstBytePartitioner.class, 3,
        messages("key",
            new byte[]{1, 2, 3}, new byte[]{2, 3, 4}, new byte[]{3, 4}));

    VoidFunction<Kryo> FACTORY = KryoCoder.FACTORY;
    KryoCoder.withKryoFactory(() -> {
      Kryo kryo = FACTORY.apply();
      kryo.addDefaultSerializer(GeneratedMessage.class, ProtobufKryo.class);
      kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
      kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
      kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
      kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
      kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
      kryo.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer());
      kryo.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
      kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
      kryo.register(InvocationHandler.class, new JdkProxySerializer());
      UnmodifiableCollectionsSerializer.registerSerializers(kryo);
      SynchronizedCollectionsSerializer.registerSerializers(kryo);
      ImmutableListSerializer.registerSerializers(kryo);
      ImmutableSetSerializer.registerSerializers(kryo);
      ImmutableMapSerializer.registerSerializers(kryo);
      ImmutableMultimapSerializer.registerSerializers(kryo);
      ReverseListSerializer.registerSerializers(kryo);
      UnmodifiableNavigableSetSerializer.registerSerializers(kryo);
      ArrayListMultimapSerializer.registerSerializers(kryo);
      HashMultimapSerializer.registerSerializers(kryo);
      LinkedHashMultimapSerializer.registerSerializers(kryo);
      LinkedListMultimapSerializer.registerSerializers(kryo);
      TreeMultimapSerializer.registerSerializers(kryo);
      return kryo;
    });
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(DirectRunner.class);
    Pipeline pipeline = Pipeline.create(opts);
    BeamFlow flow = BeamFlow.create(pipeline);
    Dataset<Integer> ds = view.observe(flow, "projects/dummy/subscriptions/dummy", new PartitionedLogObserver<Integer>() {

      @Override
      public boolean onNext(
          StreamElement ingest, PartitionedLogObserver.ConfirmCallback confirm,
          Partition partition, Consumer<Integer> collector) {

        assertEquals(ingest.getValue()[0] % 3, partition.getId());
        int sum = 0;
        for (byte b : ingest.getValue()) {
          sum += b;
        }
        collector.accept(sum);
        return true;
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    });

    PCollection<Integer> output = flow.unwrapped(ReduceWindow.of(ds)
        .combineBy(Sums.ofInts())
        .output());
    PAssert.that(output).containsInAnyOrder(6, 9, 7);
    try {
      pipeline.run();
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
    }
  }

  private List<PubsubMessage> messages(String key, byte[]... payloads) {
    return Arrays.stream(payloads)
        .map(b -> KeyValue.newBuilder()
        .setAttribute(attr.getName())
        .setKey(key)
        .setValue(ByteString.copyFrom(b))
        .build()
        .toByteArray())
        .map(b -> new PubsubMessage(b, Collections.emptyMap()))
        .collect(Collectors.toList());
  }

  private Context context() {
    return new Context(() -> Executors.newCachedThreadPool()) {
    };
  }

}
