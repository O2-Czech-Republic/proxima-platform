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
package cz.o2.proxima.beam.io;

import cz.o2.proxima.core.annotations.Experimental;
import cz.o2.proxima.core.repository.RepositoryFactory;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/** IO connector for Proxima platform. */
@Experimental
@Slf4j
public class ProximaIO {

  private ProximaIO() {
    // No-op.
  }

  /**
   * Write {@link StreamElement stream elements} into proxima using {@link DirectDataOperator}.
   *
   * @param repositoryFactory Serializable factory for Proxima repository.
   * @return Write transform.
   */
  public static Write write(RepositoryFactory repositoryFactory) {
    return new Write(repositoryFactory);
  }

  /**
   * Transformation that writes {@link StreamElement stream elements} into proxima using {@link
   * DirectDataOperator}.
   */
  public static class Write extends PTransform<PCollection<StreamElement>, PDone> {

    private final RepositoryFactory repositoryFactory;

    private Write(RepositoryFactory repositoryFactory) {
      this.repositoryFactory = repositoryFactory;
    }

    @Override
    public PDone expand(PCollection<StreamElement> input) {
      input.apply("Write", ParDo.of(new WriteFn(repositoryFactory)));
      return PDone.in(input.getPipeline());
    }
  }

  static class WriteFn extends DoFn<StreamElement, Void> {

    private final RepositoryFactory repositoryFactory;

    private transient DirectDataOperator direct;

    WriteFn(RepositoryFactory repositoryFactory) {
      this.repositoryFactory = repositoryFactory;
    }

    @VisibleForTesting
    DirectDataOperator getDirect() {
      return direct;
    }

    @Setup
    public void setUp() {
      direct = repositoryFactory.apply().getOrCreateOperator(DirectDataOperator.class);
    }

    @ProcessElement
    public void processElement(@Element StreamElement element) {
      direct
          .getWriter(element.getAttributeDescriptor())
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      String.format("Missing writer for [%s].", element.getAttributeDescriptor())))
          .write(
              element,
              (succ, error) -> {
                if (error != null) {
                  log.error(String.format("Unable to write element [%s].", element), error);
                }
              });
    }

    @Teardown
    public void tearDown() {
      if (direct != null) {
        direct.close();
      }
    }
  }
}
