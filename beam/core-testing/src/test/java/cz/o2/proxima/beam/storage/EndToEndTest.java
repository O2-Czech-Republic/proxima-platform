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

import static org.junit.Assert.assertTrue;

import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.io.ProximaIO;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.config.ConfigUtils;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.util.Optionals;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.example.Example.BaseEvent;
import cz.o2.proxima.example.Example.UserPreferences;
import cz.o2.proxima.testing.model.Model;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;

public class EndToEndTest {

  // replace all storages with in-memory testing storage
  Config conf =
      ConfigUtils.withStorageReplacement(
          ConfigFactory.parseResources("test-readme.conf").resolve(),
          name -> true,
          name -> URI.create("inmem:///" + name));
  private final Repository repo = Repository.ofTest(conf);
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final BeamDataOperator beam = repo.getOrCreateOperator(BeamDataOperator.class);
  private final Model model = Model.wrap(repo);

  public static class Transformation
      extends PTransform<PCollection<StreamElement>, PCollection<StreamElement>> {

    private final Model model;

    public Transformation(Model model) {
      this.model = model;
    }

    @Override
    public PCollection<StreamElement> expand(PCollection<StreamElement> input) {
      // transform input StreamElements to outputs

      // the inputs are supposed to be events (skipping sanity checks here)
      PCollection<BaseEvent> events =
          input.apply(
              MapElements.into(TypeDescriptor.of(BaseEvent.class))
                  .via(el -> Optionals.get(model.getEvent().getDataDescriptor().valueOf(el))));
      return events
          .apply(Reify.timestamps())
          .apply(
              MapElements.into(TypeDescriptor.of(StreamElement.class))
                  .via(
                      e ->
                          model
                              .getUser()
                              .getPreferencesDescriptor()
                              .upsert(
                                  e.getValue().getUserId(), /* user to store the event to */
                                  e.getTimestamp().getMillis(),
                                  /* store some "computed" preferences */
                                  UserPreferences.getDefaultInstance())));
    }
  }

  @Test
  public void testTransform() throws InterruptedException {
    Pipeline p = Pipeline.create();
    OnlineAttributeWriter eventWriter =
        Optionals.get(direct.getWriter(model.getEvent().getDataDescriptor()));
    long now = System.currentTimeMillis();
    writeEvent("user1", now, eventWriter);
    writeEvent("user2", now, eventWriter);
    writeEvent("user1", now + 1, eventWriter);
    PCollection<StreamElement> inputs =
        beam.getStream(p, Position.OLDEST, true, true, model.getEvent().getDataDescriptor());
    PCollection<StreamElement> outputs = inputs.apply(new Transformation(model));
    outputs.apply(ProximaIO.write(repo.asFactory()));
    p.run().waitUntilFinish();

    // when the Pipeline finishes, we should have writes in 'user1' and 'user2'

    // verify results
    RandomAccessReader reader =
        Optionals.get(direct.getRandomAccess(model.getUser().getPreferencesDescriptor()));
    List<StreamElement> read = new ArrayList<>();

    Optional<KeyValue<UserPreferences>> user1Preferences =
        reader.get("user1", model.getUser().getPreferencesDescriptor());
    Optional<KeyValue<UserPreferences>> user2Preferences =
        reader.get("user2", model.getUser().getPreferencesDescriptor());

    // we stored both the preferences
    assertTrue(user1Preferences.isPresent());
    assertTrue(user2Preferences.isPresent());
  }

  private void writeEvent(String userId, long stamp, OnlineAttributeWriter writer)
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    writer.write(
        model
            .getEvent()
            .getDataDescriptor()
            .upsert(
                UUID.randomUUID().toString(),
                stamp,
                BaseEvent.newBuilder().setUserId(userId).build()),
        (succ, exc) -> latch.countDown());
    latch.await();
  }
}
