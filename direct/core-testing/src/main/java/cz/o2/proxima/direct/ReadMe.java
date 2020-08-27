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
package cz.o2.proxima.direct;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.testing.model.Model;
import lombok.extern.slf4j.Slf4j;

/**
 * Class that uses all code that is pasted in root README.md to ensure that this code is always
 * actual.
 */
@Slf4j
class ReadMe {

  private Model createModel() {
    return Model.of(ConfigFactory.load());
  }

  private void consumeCommitLog() {
    Model model = createModel();
    DirectDataOperator operator = model.getRepo().getOrCreateOperator(DirectDataOperator.class);
    CommitLogReader commitLog =
        operator
            .getCommitLogReader(model.getEvent().getDataDescriptor())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Missing commit log for " + model.getEvent().getDataDescriptor()));
    commitLog.observe(
        "MyObservationProcess",
        new LogObserver() {

          @Override
          public boolean onError(Throwable error) {
            throw new RuntimeException(error);
          }

          @Override
          public boolean onNext(StreamElement elem, OnNextContext context) {
            log.info("Consumed element {}", elem);
            // commit processing, so that it is not redelivered
            context.confirm();
            // continue processing
            return true;
          }
        });
  }

  private ReadMe() {
    consumeCommitLog();
  }
}
