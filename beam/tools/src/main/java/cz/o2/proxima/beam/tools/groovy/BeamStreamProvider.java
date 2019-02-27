/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.tools.groovy;

import com.google.common.base.Preconditions;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.tools.groovy.Stream;
import cz.o2.proxima.tools.groovy.StreamProvider;
import cz.o2.proxima.tools.groovy.ToolsClassLoader;
import cz.o2.proxima.tools.groovy.WindowedStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * A {@link StreamProvider} for groovy tools based on beam.
 */
@Slf4j
public abstract class BeamStreamProvider implements StreamProvider {

  Repository repo;
  BeamDataOperator beam;

  @Override
  public void init(Repository repo, String[] args) {
    this.repo = repo;
    Preconditions.checkArgument(
        this.repo.hasOperator("beam"),
        "Please include proxima-beam-core dependency");
    this.beam = repo.getOrCreateOperator(BeamDataOperator.class);
  }

  @Override
  public Stream<StreamElement> getStream(
      Position position, boolean stopAtCurrent,
      boolean eventTime, TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    return BeamStream.stream(
        beam, position, stopAtCurrent, eventTime, terminateCheck,
        getJarRegisteringPipelineFactory(), attrs);
  }

  @Override
  public WindowedStream<StreamElement> getBatchUpdates(
      long startStamp, long endStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    return BeamStream.batchUpdates(
        beam, startStamp, endStamp, terminateCheck,
        getJarRegisteringPipelineFactory(), attrs);
  }

  @Override
  public WindowedStream<StreamElement> getBatchSnapshot(
      long fromStamp, long toStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    return BeamStream.batchSnapshot(
        beam, fromStamp, toStamp, terminateCheck,
        getJarRegisteringPipelineFactory(), attrs);
  }

  @Override
  public void close() {
    if (beam != null) {
      beam.close();
    }
  }

  /**
   * Create factory to be used for pipeline creation.
   * @return the factory
   */
  protected Factory<PipelineOptions> getPipelineOptionsFactory() {
    return PipelineOptionsFactory::create;
  }

  /**
   * List all UDFs created.
   * @return set of all UDFs
   */
  protected Set<String> listUdfClassNames() {
    ToolsClassLoader loader = (ToolsClassLoader) Thread.currentThread()
        .getContextClassLoader();
    return loader.getDefinedClasses();
  }

  Factory<Pipeline> getJarRegisteringPipelineFactory() {
    Factory<PipelineOptions> factory = getPipelineOptionsFactory();
    UnaryFunction<PipelineOptions, Pipeline> createPipeline = getCreatePipelineFromOpts();
    return () -> {
      PipelineOptions opts = factory.apply();
      createUdfJarAndRegisterToPipeline(opts);
      return createPipeline.apply(opts);
    };
  }

  /**
   * Convert {@link PipelineOptions} into {@link Pipeline}.
   * @return function to use for creating pipeline from options
   */
  protected UnaryFunction<PipelineOptions, Pipeline> getCreatePipelineFromOpts() {
    return Pipeline::create;
  }

  /**
   * Create jar from UDFs and register this jar into pipeline
   * @param opts the pipeline to register jar for
   */
  void createUdfJarAndRegisterToPipeline(PipelineOptions opts) {
    String runnerName = opts.getRunner().getSimpleName();
    try {
      File path = createJarFromUdfs();
      log.info("Injecting generated jar at {} into {}", path, runnerName);
      switch (runnerName) {
        case "DirectRunner":
        case "FlinkRunner":
          injectJarIntoClassloader(
              (URLClassLoader) opts.getRunner().getClassLoader(), path);
          break;
        case "SparkRunner":
          throw new UnsupportedOperationException("Spark unsupported for now.");
        default:
          throw new IllegalStateException(
              "Don't know how to inject jar into " + runnerName);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private File createJarFromUdfs() throws IOException {
    Set<String> classes = listUdfClassNames();
    ToolsClassLoader loader = (ToolsClassLoader) Thread
        .currentThread().getContextClassLoader();
    log.info(
        "Building jar from classes {} retrieved from {}",
        classes, loader);
    File out = File.createTempFile("proxima-tools", ".jar");

    out.deleteOnExit();
    try (JarOutputStream output = new JarOutputStream(new FileOutputStream(out))) {
      long now = System.currentTimeMillis();
      for (String cls : classes) {
        String name = cls.replace('.', '/') + ".class";
        JarEntry entry = new JarEntry(name);
        entry.setTime(now);
        output.putNextEntry(entry);
        InputStream input = new ByteArrayInputStream(loader.getClassByteCode(cls));
        IOUtils.copy(input, output);
        output.closeEntry();
      }
    }
    return out;
  }

  // this is hackish, but we need to inject the jar into ClassLoader
  // that loaded the runner class
  private void injectJarIntoClassloader(URLClassLoader loader, File path) {
    try {
      Method addUrl = URLClassLoader.class.getDeclaredMethod(
          "addURL", new Class[] { URL.class });
      addUrl.setAccessible(true);
      addUrl.invoke(loader, new URL("file://" + path.getAbsolutePath()));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
