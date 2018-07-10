/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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

import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.TransformationDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.LogObserver;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for running transformations locally.
 */
@Slf4j
public class TransformationRunner {

  /**
   * Run all transformations in given repository.
   * @param repo the repository
   */
  public static void runTransformations(Repository repo) {
    repo.getTransformations()
        .forEach((name, desc) -> runTransformation(repo, name, desc, i -> { }));
  }

  /**
   * Run all transformations in given repository.
   * @param repo the repository
   * @param onReplicate callback to be called before write to replicated target
   */
  public static void runTransformations(
      Repository repo,
      Consumer<StreamElement> onReplicate) {

    repo.getTransformations()
        .forEach((name, desc) -> runTransformation(repo, name, desc, onReplicate));
  }


  /**
   * Run given transformation in local JVM.
   * @param repo the repository
   * @param name name of the transformation
   * @param desc the transformation to run
   * @param onReplicate callback to be called before write to replicated target
   */
  public static void runTransformation(
      Repository repo,
      String name,
      TransformationDescriptor desc,
      Consumer<StreamElement> onReplicate) {

    desc.getAttributes().stream()
        .flatMap(attr -> repo.getFamiliesForAttribute(attr)
            .stream()
            .filter(af -> af.getAccess().canReadCommitLog()))
        .collect(Collectors.toSet())
        .stream()
        .findAny()
        .flatMap(AttributeFamilyDescriptor::getCommitLogReader)
        .orElseThrow(() -> new IllegalStateException(
            "No commit log reader for attributes of transformation " + desc))
        .observe(name, new LogObserver() {
          @Override
          public boolean onNext(StreamElement ingest, LogObserver.OffsetCommitter committer) {
            desc.getTransformation().apply(ingest, transformed -> {
              onReplicate.accept(transformed);
              repo.getWriter(transformed.getAttributeDescriptor())
                  .get()
                  .write(transformed, committer::commit);
            });
            return true;
          }

          @Override
          public boolean onError(Throwable error) {
            log.error("Error in transformer {}", name, error);
            throw new RuntimeException(error);
          }

        });
  }

  private TransformationRunner() { }

}
