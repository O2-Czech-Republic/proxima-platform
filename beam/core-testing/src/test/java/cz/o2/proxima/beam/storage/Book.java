/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.storage;

import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.io.ProximaIO;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.example.Example.BaseEvent;
import cz.o2.proxima.example.Example.BaseEvent.Action;
import cz.o2.proxima.testing.model.Model;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Code from docs. */
public class Book {

  private Repository repo;
  private BeamDataOperator beam;

  public void createOperator() {
    repo = Repository.of(ConfigFactory.load("test-readme.conf").resolve());
    beam = repo.getOrCreateOperator(BeamDataOperator.class);
  }

  public void readFromPlatform() {
    final Model model = Model.wrap(repo);
    Pipeline p = Pipeline.create();
    PCollection<StreamElement> events =
        beam.getStream(
            p,
            Position.OLDEST,
            false, /* stopAtCurrent */
            true, /* useEventTime */
            model.getEvent().getDataDescriptor());
    PCollection<BaseEvent> parsed =
        events.apply(
            FlatMapElements.into(TypeDescriptor.of(BaseEvent.class))
                .via(
                    e ->
                        model.getEvent().getDataDescriptor().valueOf(e).stream()
                            .collect(Collectors.toList())));
    // further processing
    // ...

    // run the Pipeline
    p.run().waitUntilFinish();
  }

  public void proximaSink() {
    Pipeline p = Pipeline.create();
    final Model model = Model.wrap(repo);
    PCollection<StreamElement> elements =
        beam.getStream(
            "InputEvents", p, Position.CURRENT, false, true, model.getEvent().getDataDescriptor());

    // implement actual transformation logic here
    // convert the outputs to StreamElements
    PCollection<StreamElement> outputs =
        elements
            .apply(
                FlatMapElements.into(TypeDescriptor.of(BaseEvent.class))
                    .via(
                        e ->
                            model.getEvent().getDataDescriptor().valueOf(e).stream()
                                .collect(Collectors.toList())))
            .apply(Filter.by(e -> e.getAction().equals(Action.BUY)))
            .apply(new ProcessUserBuys());

    // store the result
    outputs.apply(ProximaIO.write(repo.asFactory()));
    // run the Pipeline
    p.run().waitUntilFinish();
  }

  private static class ProcessUserBuys
      extends PTransform<PCollection<BaseEvent>, PCollection<StreamElement>> {
    @Override
    public PCollection<StreamElement> expand(PCollection<BaseEvent> input) {
      // implement logic here
      return null;
    }
  }
}
