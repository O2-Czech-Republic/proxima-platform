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
package cz.o2.proxima.beam.io;

import cz.o2.proxima.beam.core.ProximaPipelineOptions;
import cz.o2.proxima.core.annotations.Experimental;
import cz.o2.proxima.core.repository.RepositoryFactory;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.direct.core.transaction.TransactionalOnlineAttributeWriter.TransactionRejectedException;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
      long bundleFinalizeTimeoutMs =
          input
              .getPipeline()
              .getOptions()
              .as(ProximaPipelineOptions.class)
              .getProximaIOWriteFinalizeTimeoutMs();
      input.apply("Write", ParDo.of(new WriteFn(bundleFinalizeTimeoutMs, repositoryFactory)));
      return PDone.in(input.getPipeline());
    }
  }

  static class WriteFn extends DoFn<StreamElement, Void> {

    private final RepositoryFactory repositoryFactory;
    private final long bundleFinalizeTimeoutMs;

    private transient DirectDataOperator direct;

    private transient Set<CompletableFuture<Pair<Boolean, Throwable>>> pendingWrites;
    private transient AtomicInteger missingResponses;

    WriteFn(long bundleFinalizeTimeoutMs, RepositoryFactory repositoryFactory) {
      this.bundleFinalizeTimeoutMs = bundleFinalizeTimeoutMs;
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

    @StartBundle
    public void startBundle() {
      // we access the collection asynchronously on completion of writes
      pendingWrites = Collections.synchronizedSet(new HashSet<>());
      missingResponses = new AtomicInteger();
    }

    @FinishBundle
    public void finishBundle() {
      long startTime = System.currentTimeMillis();
      while (missingResponses.get() > 0) {
        long elapsed = System.currentTimeMillis() - startTime;
        if (elapsed >= bundleFinalizeTimeoutMs) {
          throw new IllegalStateException("Failed to flush bundle within timeout of 5s");
        }
        // clone to avoid ConcurrentModificationException
        final Collection<CompletableFuture<Pair<Boolean, Throwable>>> unfinished;
        synchronized (pendingWrites) {
          unfinished = new ArrayList<>(pendingWrites);
          pendingWrites.clear();
        }
        Optional<Pair<Boolean, Throwable>> failedFuture =
            unfinished.stream()
                .map(
                    f ->
                        ExceptionUtils.uncheckedFactory(
                            () -> f.get(bundleFinalizeTimeoutMs - elapsed, TimeUnit.MILLISECONDS)))
                .filter(p -> !p.getFirst())
                // this will be retried
                .filter(p -> !(p.getSecond() instanceof TransactionRejectedException))
                .findAny();
        if (failedFuture.isPresent()) {
          throw new IllegalStateException(failedFuture.get().getSecond());
        }
      }
      // bundle finished
      pendingWrites = null;
    }

    @ProcessElement
    public void processElement(@Element StreamElement element) {
      OnlineAttributeWriter writer = getWriterForElement(element);
      AtomicReference<Runnable> writeRunnableRef = new AtomicReference<>();
      // increment missing responses outside the retry runnable
      missingResponses.incrementAndGet();
      writeRunnableRef.set(
          () -> {
            CompletableFuture<Pair<Boolean, Throwable>> writeResult = new CompletableFuture<>();
            writeResult.thenAccept(
                r -> {
                  if (Boolean.TRUE.equals(r.getFirst())) {
                    // remove successfully completed write
                    missingResponses.decrementAndGet();
                    pendingWrites.remove(writeResult);
                  } else if (r.getSecond() instanceof TransactionRejectedException) {
                    // restart the writing transaction
                    writeRunnableRef.get().run();
                    // transaction rejected, restart transaction
                    pendingWrites.remove(writeResult);
                  }
                  // else keep the failed future until finish bundle
                });
            pendingWrites.add(writeResult);
            writer.write(
                element,
                (succ, error) -> {
                  writeResult.complete(Pair.of(succ, error));
                  if (error != null) {
                    log.error("Unable to write element [{}]", element, error);
                  }
                });
          });
      // run the runnable
      writeRunnableRef.get().run();
    }

    @VisibleForTesting
    OnlineAttributeWriter getWriterForElement(StreamElement element) {
      return direct
          .getWriter(element.getAttributeDescriptor())
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      String.format("Missing writer for [%s].", element.getAttributeDescriptor())));
    }

    @Teardown
    public void tearDown() {
      if (direct != null) {
        direct.close();
      }
    }
  }
}
