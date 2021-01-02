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

import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import java.util.List;
import java.util.stream.Collectors;
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
              OnlineAttributeWriter writer =
                  af.getWriter()
                      .orElseThrow(
                          () -> new IllegalStateException("Missing writer of family " + af))
                      .online();
              ObserveHandle handle =
                  attributes
                      .stream()
                      .map(
                          a ->
                              direct
                                  .getFamiliesForAttribute(a)
                                  .stream()
                                  .filter(f -> f.getDesc().getType() == StorageType.PRIMARY)
                                  .findAny()
                                  .get())
                      .collect(Collectors.toSet())
                      .stream()
                      .findFirst()
                      .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader)
                      .get()
                      .observe(
                          af.getDesc().getName(),
                          new LogObserver() {
                            @Override
                            public boolean onNext(StreamElement ingest, OnNextContext context) {
                              log.debug("Replicating input {} to {}", ingest, writer);
                              writer.write(
                                  ingest,
                                  (succ, exc) -> {
                                    context.commit(succ, exc);
                                    onReplicated.accept(ingest);
                                  });
                              return true;
                            }

                            @Override
                            public boolean onError(Throwable error) {
                              throw new RuntimeException(error);
                            }
                          });
              ExceptionUtils.unchecked(handle::waitUntilReady);
              log.info("Started attribute replica {}", af.getDesc().getName());
            });
  }

  private ReplicationRunner() {}
}
