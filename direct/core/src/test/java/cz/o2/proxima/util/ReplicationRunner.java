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
package cz.o2.proxima.util;

import cz.o2.proxima.direct.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReplicationRunner {

  /**
   * Run replications of attributes to replica attribute families.
   *
   * @param direct {@link DirectDataOperator} direct data operator
   */
  public static void runAttributeReplicas(DirectDataOperator direct) {
    runAttributeReplicas(direct, el -> {});
  }

  /**
   * Run replications of attributes to replica attribute families.
   *
   * @param direct {@link DirectDataOperator} direct data operator
   * @param onReplicated callback called for each replicated element
   */
  public static void runAttributeReplicas(
      DirectDataOperator direct, Consumer<StreamElement> onReplicated) {
    direct
        .getAllFamilies()
        .filter(af -> af.getDesc().getType() == StorageType.REPLICA)
        .filter(af -> !af.getDesc().getAccess().isReadonly() && !af.getDesc().isProxy())
        .forEach(
            af -> {
              List<AttributeDescriptor<?>> attributes = af.getAttributes();
              final AttributeWriterBase writer = Optionals.get(af.getWriter());
              final CommitLogReader primaryCommitLogReader =
                  Optionals.get(
                      attributes
                          .stream()
                          .flatMap(a -> direct.getFamiliesForAttribute(a).stream())
                          .filter(f -> f.getDesc().getType() == StorageType.PRIMARY)
                          .distinct()
                          .findFirst()
                          .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader));
              final ObserveHandle handle;
              if (writer instanceof OnlineAttributeWriter) {
                final OnlineAttributeWriter onlineWriter = writer.online();
                handle =
                    primaryCommitLogReader.observe(
                        af.getDesc().getName(),
                        (CommitLogObserver)
                            (ingest, context) -> {
                              log.debug("Replicating input {} to {}", ingest, writer);
                              onlineWriter.write(
                                  ingest,
                                  (succ, exc) -> {
                                    context.commit(succ, exc);
                                    onReplicated.accept(ingest);
                                  });
                              return true;
                            });
              } else {
                final BulkAttributeWriter bulkWriter = writer.bulk();
                handle =
                    primaryCommitLogReader.observe(
                        af.getDesc().getName(),
                        (CommitLogObserver)
                            (ingest, context) -> {
                              log.debug("Replicating input {} to {}", ingest, writer);
                              bulkWriter.write(
                                  ingest,
                                  context.getWatermark(),
                                  (succ, exc) -> {
                                    context.commit(succ, exc);
                                    onReplicated.accept(ingest);
                                  });
                              return true;
                            });
              }
              ExceptionUtils.unchecked(handle::waitUntilReady);
              log.info("Started attribute replica {}", af.getDesc().getName());
            });
  }

  private ReplicationRunner() {}
}
