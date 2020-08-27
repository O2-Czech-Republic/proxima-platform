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
package cz.o2.proxima.beam;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.example.Example.BaseEvent;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.testing.model.Model;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.joda.time.Duration;

/**
 * Class that uses all code that is pasted in root README.md to ensure that this code is always
 * actual.
 */
@Slf4j
class ReadMe {

  private Model createModel() {
    return Model.of(ConfigFactory.defaultApplication());
  }

  private void createStream() {
    Model model = createModel();
    BeamDataOperator operator = model.getRepo().getOrCreateOperator(BeamDataOperator.class);
    Pipeline pipeline = Pipeline.create();
    PCollection<StreamElement> input =
        operator.getStream(
            pipeline, Position.OLDEST, false, true, model.getEvent().getDataDescriptor());
    PCollection<KV<String, Long>> counted =
        CountByKey.of(input)
            .keyBy(
                el ->
                    model
                        .getEvent()
                        .getDataDescriptor()
                        .valueOf(el)
                        .map(BaseEvent::getProductId)
                        .orElse(""))
            .windowBy(FixedWindows.of(Duration.standardMinutes(1)))
            .triggeredBy(AfterWatermark.pastEndOfWindow())
            .accumulationMode(AccumulationMode.DISCARDING_FIRED_PANES)
            .output();
    // do something with the output
  }

  private ReadMe() {
    createStream();
  }
}
