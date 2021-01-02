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
package cz.o2.proxima.beam.core;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public class JoinProxyTransform extends BeamProxyTransform {

  private AttributeDescriptor<byte[]> status;
  private AttributeDescriptor<byte[]> armed;
  private AttributeDescriptor<Integer> proxy;
  private transient BeamDataOperator beam;

  @Override
  public void setup(EntityDescriptor entity, BeamDataOperator beamDataOperator) {
    status = entity.getAttribute("status");
    armed = entity.getAttribute("armed");
    proxy = entity.getAttribute("armed-proxy");
    beam = beamDataOperator;
  }

  @Override
  public PCollection<StreamElement> createStream(
      String name,
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      long limit) {

    PCollection<StreamElement> input =
        beam.getStream(name, pipeline, position, stopAtCurrent, eventTime, limit, status, armed);
    return applyJoin(input);
  }

  @Override
  public PCollection<StreamElement> createStreamFromUpdates(
      Pipeline pipeline, long startStamp, long endStamp, long limit) {
    PCollection<StreamElement> input =
        beam.getBatchUpdates(pipeline, startStamp, endStamp, true, status, armed);
    return applyJoin(input);
  }

  @Override
  public PCollection<StreamElement> createBatch(Pipeline pipeline, long startStamp, long endStamp) {
    PCollection<StreamElement> input =
        beam.getBatchUpdates(pipeline, startStamp, endStamp, status, armed);
    return applyJoin(input);
  }

  PCollection<StreamElement> applyJoin(PCollection<StreamElement> input) {
    return input.apply(toKvs()).apply(ParDo.of(filterWhenMissingStatus(status, proxy)));
  }

  private static PTransform<PCollection<StreamElement>, PCollection<KV<String, StreamElement>>>
      toKvs() {
    return new PTransform<PCollection<StreamElement>, PCollection<KV<String, StreamElement>>>() {
      @Override
      public PCollection<KV<String, StreamElement>> expand(PCollection<StreamElement> input) {
        return input.apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), TypeDescriptor.of(StreamElement.class)))
                .via(el -> KV.of(el.getKey(), el)));
      }
    };
  }

  private static DoFn<KV<String, StreamElement>, StreamElement> filterWhenMissingStatus(
      AttributeDescriptor<byte[]> status, AttributeDescriptor<Integer> proxy) {
    return new DoFn<KV<String, StreamElement>, StreamElement>() {
      @StateId("status")
      final StateSpec<ValueState<Boolean>> spec = StateSpecs.value();

      @ProcessElement
      @RequiresTimeSortedInput
      public void process(
          @Element KV<String, StreamElement> el,
          @StateId("status") ValueState<Boolean> state,
          OutputReceiver<StreamElement> output) {
        if (el.getValue().getAttributeDescriptor().equals(status)) {
          state.write(true);
        } else if (Optional.ofNullable(state.read()).orElse(false)) {
          output.output(toProxyAttr(el.getValue(), proxy));
        }
      }
    };
  }

  static StreamElement toProxyAttr(StreamElement value, AttributeDescriptor<Integer> proxy) {
    return StreamElement.upsert(
        value.getEntityDescriptor(),
        proxy,
        value.getUuid(),
        value.getKey(),
        proxy.getName(),
        value.getStamp(),
        proxy.getValueSerializer().serialize(value.getValue().length));
  }
}
