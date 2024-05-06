/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct;

import com.google.protobuf.TextFormat;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.view.CachedView;
import cz.o2.proxima.example.Example.BaseEvent;
import cz.o2.proxima.example.Example.BaseEvent.Action;
import cz.o2.proxima.example.Example.UserDetails;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.testing.model.Model;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/** Class covering code in documentation. */
@Slf4j
public class Book {

  private ExecutorService executor = Executors.newCachedThreadPool();

  private Repository repo;
  private DirectDataOperator direct;
  private Model model;

  public void createOperator() {
    repo = Repository.of(ConfigFactory.load("test-readme.conf").resolve());
    model = Model.wrap(repo);
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
  }

  public void writeData() {
    Optional<OnlineAttributeWriter> maybeWriter =
        direct.getWriter(model.getEvent().getDataDescriptor());
    Preconditions.checkState(maybeWriter.isPresent());
    OnlineAttributeWriter writer = maybeWriter.get();

    // create event describing user 'user' buying product 'product'
    BaseEvent event =
        BaseEvent.newBuilder()
            .setProductId("product")
            .setUserId("user")
            .setAction(Action.BUY)
            .build();

    // create StreamElement for the event
    StreamElement element =
        model
            .getEvent()
            .getDataDescriptor()
            .upsert(UUID.randomUUID().toString(), System.currentTimeMillis(), event);

    // write the event, will be confirmed asynchronously
    writer.write(
        element,
        (succ, exc) -> {
          if (succ) {
            log.info("Event successfully written.");
          } else {
            log.warn("Error during writing of element", exc);
          }
        });
  }

  public void observeEvents() {
    Optional<CommitLogReader> maybeReader =
        direct.getCommitLogReader(model.getEvent().getDataDescriptor());
    Preconditions.checkState(maybeReader.isPresent());
    CommitLogReader reader = maybeReader.get();
    reader.observe(
        // name the observer, if multiple observers with the same name exist
        // the events will be load balanced among them
        "EventsProcessor",
        new CommitLogObserver() {
          @Override
          public boolean onNext(StreamElement element, OnNextContext context) {
            Optional<BaseEvent> maybeEvent = model.getEvent().getDataDescriptor().valueOf(element);
            if (maybeEvent.isPresent()) {
              // successfully parsed the event value
              log.info("Received event {}", TextFormat.shortDebugString(maybeEvent.get()));
              // run some logic to handle the event
              handleEvent(maybeEvent.get(), context);
            } else {
              log.warn("Failed to parse value from {}", element);
              // confirm the element was processed
              context.confirm();
            }
            return false;
          }

          private void handleEvent(BaseEvent event, OnNextContext context) {
            // do some logic
            // can be asynchronous
            executor.submit(
                () -> {
                  ExceptionUtils.unchecked(() -> TimeUnit.SECONDS.sleep(1));
                  log.info("Event {} processed.", TextFormat.shortDebugString(event));
                  // do not forget to confirm the processing
                  context.confirm();
                });
          }
        });
  }

  public void randomRead() {
    Optional<RandomAccessReader> maybeReader =
        direct.getRandomAccess(model.getUser().getDetailsDescriptor());
    Preconditions.checkState(maybeReader.isPresent());
    RandomAccessReader reader = maybeReader.get();
    String userId = "user";
    Optional<KeyValue<UserDetails>> maybeUserDetails =
        reader.get(userId, model.getUser().getDetailsDescriptor());
    if (maybeUserDetails.isPresent()) {
      // process retrieved details
      // KeyValue extends StreamElement, but is already typed
      KeyValue<UserDetails> detailsKv = maybeUserDetails.get();
      // failure to parse would throw exception
      UserDetails userDetails = detailsKv.getParsedRequired();

      log.info(
          "Retrieved details {} for user {}", TextFormat.shortDebugString(userDetails), userId);
    } else {
      log.info("User {} has no details", userId);
    }
  }

  public void cachedView() {
    Optional<CachedView> maybeView = direct.getCachedView(model.getUser().getDetailsDescriptor());
    if (maybeView.isEmpty()) {
      log.warn(
          "Cannot create cached view. There must be family with access 'cached-view' defined.");
    } else {
      CachedView view = maybeView.get();
      // read all partitions of the underlying storage
      // can be used to select only a subset of partitions

      // this call will block until the data is cached
      view.assign(view.getPartitions());

      // read the user details
      String userId = "user";
      Optional<KeyValue<UserDetails>> maybeDetails =
          view.get("user", model.getUser().getDetailsDescriptor());
      if (maybeDetails.isPresent()) {
        log.info("Have details {} for user {}", maybeDetails.get().getParsedRequired(), userId);
      } else {
        log.info("User {} has no details", userId);
      }
    }
  }
}
