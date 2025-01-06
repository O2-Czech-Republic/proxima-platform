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
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.TransformationDescriptor;
import cz.o2.proxima.core.repository.TransformationDescriptor.InputTransactionMode;
import cz.o2.proxima.core.repository.TransformationDescriptor.OutputTransactionMode;
import cz.o2.proxima.core.storage.StorageType;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.commitlog.CommitLogObserver;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.commitlog.ObserveHandle;
import cz.o2.proxima.direct.core.transform.DirectElementWiseTransform;
import cz.o2.proxima.direct.core.transform.TransformationObserver;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/** Utility class for running transformations locally. */
@Slf4j
public class TransformationRunner {

  /**
   * Run all transformations in given repository.
   *
   * @param repo the repository
   * @param direct the operator to run transformations with
   */
  public static void runTransformations(Repository repo, DirectDataOperator direct) {
    repo.getTransformations().entrySet().stream()
        .filter(e -> e.getValue().getInputTransactionMode() != InputTransactionMode.TRANSACTIONAL)
        .forEach(e -> runTransformation(direct, e.getKey(), e.getValue(), i -> {}));
  }

  /**
   * Run all transformations in given repository.
   *
   * @param repo the repository
   * @param direct the operator to run transformations with
   * @param onReplicated callback to be called before write to replicated target
   */
  public static void runTransformations(
      Repository repo, DirectDataOperator direct, Consumer<StreamElement> onReplicated) {

    repo.getTransformations().entrySet().stream()
        .filter(e -> e.getValue().getInputTransactionMode() != InputTransactionMode.TRANSACTIONAL)
        .map(
            entry ->
                Pair.of(
                    entry.getKey(),
                    runTransformation(direct, entry.getKey(), entry.getValue(), onReplicated)))
        .forEach(
            p -> {
              ExceptionUtils.unchecked(p.getSecond()::waitUntilReady);
              log.info("Started transformation {}", p.getFirst());
            });
  }

  /**
   * Run given transformation in local JVM.
   *
   * @param direct the operator to run transformations with
   * @param name name of the transformation
   * @param desc the transformation to run
   * @param onReplicated callback to be called before write to replicated target
   * @return {@link ObserveHandle} of the transformation
   */
  public static ObserveHandle runTransformation(
      DirectDataOperator direct,
      String name,
      TransformationDescriptor desc,
      Consumer<StreamElement> onReplicated) {

    final CommitLogObserver observer;
    if (desc.getTransformation().isContextual()) {
      observer =
          new TransformationObserver.Contextual(
              direct,
              name,
              desc.getTransformation().as(DirectElementWiseTransform.class),
              desc.getOutputTransactionMode() == OutputTransactionMode.ENABLED,
              desc.getFilter()) {

            @Override
            protected void onReplicated(StreamElement element) {
              onReplicated.accept(element);
            }
          };
    } else {
      observer =
          new TransformationObserver.NonContextual(
              direct,
              name,
              desc.getTransformation().asElementWiseTransform(),
              desc.getOutputTransactionMode() == OutputTransactionMode.ENABLED,
              desc.getFilter()) {

            @Override
            protected void onReplicated(StreamElement element) {
              onReplicated.accept(element);
            }
          };
    }

    CommitLogReader reader =
        desc.getAttributes().stream()
            .flatMap(attr -> findFamilyDescriptorForAttribute(direct, attr))
            .findAny()
            .flatMap(DirectAttributeFamilyDescriptor::getCommitLogReader)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "No commit log reader for attributes of transformation " + desc));
    log.debug("Starting to observe reader {} with observer {} as {}", reader, observer, name);
    return reader.observe(name, observer);
  }

  private static Stream<DirectAttributeFamilyDescriptor> findFamilyDescriptorForAttribute(
      DirectDataOperator direct, AttributeDescriptor<?> attr) {

    EntityDescriptor entity = direct.getRepository().getEntity(attr.getEntity());
    if (entity.isSystemEntity()) {
      return direct
          .getRepository()
          .getAllFamilies(true)
          .filter(af -> af.getEntity().equals(entity))
          .filter(af -> af.getAttributes().contains(attr))
          .filter(af -> af.getType() == StorageType.PRIMARY)
          .map(af -> direct.getFamilyByName(af.getName()));
    }
    return direct.getFamiliesForAttribute(attr).stream()
        .filter(af -> af.getDesc().getAccess().canReadCommitLog());
  }

  private TransformationRunner() {}
}
