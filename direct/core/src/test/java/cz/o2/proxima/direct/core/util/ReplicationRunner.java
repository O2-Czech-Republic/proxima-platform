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
package cz.o2.proxima.core.util;

import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.functional.TriFunction;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.storage.StorageType;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.AttributeWriterBase;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver.OnNextContext;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import java.net.URI;
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
                      attributes.stream()
                          .flatMap(a -> direct.getFamiliesForAttribute(a).stream())
                          .filter(f -> f.getDesc().getType() == StorageType.PRIMARY)
                          .distinct()
                          .findFirst()
                          .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader));
              final ObserveHandle handle;
              if (writer instanceof OnlineAttributeWriter) {
                handle =
                    primaryCommitLogReader.observe(
                        af.getDesc().getName(),
                        newReplicationCommitLogObserver(attributes, writer.online(), onReplicated));
              } else {
                handle =
                    primaryCommitLogReader.observe(
                        af.getDesc().getName(),
                        newReplicationCommitLogObserver(attributes, writer.bulk(), onReplicated));
              }
              ExceptionUtils.unchecked(handle::waitUntilReady);
              log.info("Started attribute replica {}", af.getDesc().getName());
            });
  }

  static CommitLogObserver newReplicationCommitLogObserver(
      List<AttributeDescriptor<?>> attributes,
      OnlineAttributeWriter writer,
      Consumer<StreamElement> onReplicated) {

    return newReplicationObserver(
        (ingest, context, commit) -> {
          writer.write(ingest, commit);
          return null;
        },
        writer.getUri(),
        attributes,
        onReplicated);
  }

  static CommitLogObserver newReplicationCommitLogObserver(
      List<AttributeDescriptor<?>> attributes,
      BulkAttributeWriter writer,
      Consumer<StreamElement> onReplicated) {

    return newReplicationObserver(
        (ingest, context, commit) -> {
          writer.write(ingest, context.getWatermark(), commit);
          return null;
        },
        writer.getUri(),
        attributes,
        onReplicated);
  }

  private static CommitLogObserver newReplicationObserver(
      TriFunction<StreamElement, OnNextContext, CommitCallback, Void> write,
      URI uri,
      List<AttributeDescriptor<?>> attributes,
      Consumer<StreamElement> onReplicated) {

    return (ingest, context) -> {
      if (attributes.contains(ingest.getAttributeDescriptor())) {
        write.apply(
            ingest,
            context,
            (succ, exc) -> {
              log.debug("Replicated input {} to {}", ingest, uri);
              context.commit(succ, exc);
              onReplicated.accept(ingest);
            });
      }
      return true;
    };
  }

  private ReplicationRunner() {}
}
