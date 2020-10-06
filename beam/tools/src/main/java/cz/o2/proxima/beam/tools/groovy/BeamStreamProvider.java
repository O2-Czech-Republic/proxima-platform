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
package cz.o2.proxima.beam.tools.groovy;

import com.google.api.client.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
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
import cz.o2.proxima.util.Classpath;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.runners.core.construction.resources.PipelineResources;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** A {@link StreamProvider} for groovy tools based on beam. */
@Slf4j
public abstract class BeamStreamProvider implements StreamProvider {

  @FunctionalInterface
  public interface RunnerRegistrar {
    void apply(PipelineOptions opts);
  }

  public static class Default extends BeamStreamProvider {

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private String[] args;

    @Nullable private String runner = null;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final List<RunnerRegistrar> registrars = new ArrayList<>();

    @Override
    public void init(Repository repo, String[] args) {
      args = readAndRemoveRegistrars(args, registrars);
      super.init(repo, args);
      this.args = args;
      runner = System.getenv("RUNNER");
      log.info(
          "Created {} with arguments {} and env RUNNER {}",
          getClass().getName(),
          Arrays.toString(args),
          runner);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected Supplier<PipelineOptions> getPipelineOptionsFactory() {
      return () -> {
        PipelineOptions opts = PipelineOptionsFactory.fromArgs(args).create();
        if (runner != null) {
          opts.setRunner((Class) Classpath.findClass(runner, PipelineRunner.class));
        }
        registrars.forEach(r -> r.apply(opts));
        return opts;
      };
    }

    private String[] readAndRemoveRegistrars(String[] args, List<RunnerRegistrar> registrars) {

      List<String> argsList = Arrays.stream(args).collect(Collectors.toList());
      List<String> remaining = new ArrayList<>();
      for (String arg : argsList) {
        if (arg.startsWith("--runnerRegistrar=")) {
          registrars.add(Classpath.newInstance(arg.substring(18), RunnerRegistrar.class));
        } else {
          remaining.add(arg);
        }
      }
      return remaining.toArray(new String[] {});
    }
  }

  Repository repo;
  BeamDataOperator beam;

  @Override
  public void init(Repository repo, String[] args) {
    this.repo = repo;
    Preconditions.checkArgument(
        this.repo.hasOperator("beam"), "Please include proxima-beam-core dependency");
    this.beam = repo.getOrCreateOperator(BeamDataOperator.class);
  }

  @Override
  public Stream<StreamElement> getStream(
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    return BeamStream.stream(
        beam,
        position,
        stopAtCurrent,
        eventTime,
        terminateCheck,
        getJarRegisteringPipelineFactory(),
        attrs);
  }

  @Override
  public WindowedStream<StreamElement> getBatchUpdates(
      long startStamp,
      long endStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    return BeamStream.batchUpdates(
        beam, startStamp, endStamp, terminateCheck, getJarRegisteringPipelineFactory(), attrs);
  }

  @Override
  public WindowedStream<StreamElement> getBatchSnapshot(
      long fromStamp,
      long toStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    return BeamStream.batchSnapshot(
        beam, fromStamp, toStamp, terminateCheck, getJarRegisteringPipelineFactory(), attrs);
  }

  @Override
  public void close() {
    if (beam != null) {
      beam.close();
    }
  }

  /**
   * Create factory to be used for pipeline creation.
   *
   * @return the factory
   */
  protected Supplier<PipelineOptions> getPipelineOptionsFactory() {
    return PipelineOptionsFactory::create;
  }

  /**
   * List all UDFs created.
   *
   * @return set of all UDFs
   */
  protected Set<String> listUdfClassNames() {
    return Optional.ofNullable(getToolsClassLoader())
        .map(ToolsClassLoader::getDefinedClasses)
        .orElse(Collections.emptySet());
  }

  Factory<Pipeline> getJarRegisteringPipelineFactory() {
    Supplier<PipelineOptions> factory = getPipelineOptionsFactory();
    UnaryFunction<PipelineOptions, Pipeline> createPipeline = getCreatePipelineFromOpts();
    return () -> {
      PipelineOptions opts = factory.get();
      ExperimentalOptions experimentOpts = opts.as(ExperimentalOptions.class);
      ArrayList<String> experiments =
          Lists.newArrayList(
              MoreObjects.firstNonNull(experimentOpts.getExperiments(), new ArrayList<>()));
      experiments.add("use_deprecated_read");
      experimentOpts.setExperiments(experiments);
      Pipeline pipeline = createPipeline.apply(opts);
      createUdfJarAndRegisterToPipeline(pipeline.getOptions());
      return pipeline;
    };
  }

  /**
   * Convert {@link PipelineOptions} into {@link Pipeline}.
   *
   * @return function to use for creating pipeline from options
   */
  protected UnaryFunction<PipelineOptions, Pipeline> getCreatePipelineFromOpts() {
    return Pipeline::create;
  }

  /**
   * Create jar from UDFs and register this jar into pipeline
   *
   * @param opts the pipeline to register jar for
   */
  void createUdfJarAndRegisterToPipeline(PipelineOptions opts) {
    String runnerName = opts.getRunner().getSimpleName();
    try {
      File path = createJarFromUdfs();
      URL url = new URL("file://" + path.getAbsolutePath());
      log.info("Injecting generated jar at {} into {}", path, runnerName);
      injectJarIntoContextClassLoader(url);
      if (opts.getRunner().getClassLoader() instanceof URLClassLoader) {
        // this is fallback
        injectUrlIntoClassloader((URLClassLoader) opts.getRunner().getClassLoader(), url);
      }
      if (runnerName.equals("SparkRunner")) {
        List<String> filesToStage =
            PipelineResources.detectClassPathResourcesToStage(
                Thread.currentThread().getContextClassLoader(), opts);
        log.info("Injecting generated jar {} into SparkRunner.filesToStage", url.toExternalForm());
        SparkCommonPipelineOptions sparkOpts = opts.as(SparkCommonPipelineOptions.class);
        filesToStage.add(url.toExternalForm());
        sparkOpts.setFilesToStage(
            filesToStage
                .stream()
                .filter(
                    f -> {
                      URI uri = URI.create(f);
                      if (!uri.isAbsolute()) {
                        uri = URI.create("file:///" + f);
                      }
                      return !new File(uri).isDirectory();
                    })
                .collect(Collectors.toList()));
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private File createJarFromUdfs() throws IOException {
    Set<String> classes = listUdfClassNames();
    File out = File.createTempFile("proxima-tools", ".jar");
    ToolsClassLoader loader = getToolsClassLoader();
    log.info("Building jar from classes {} retrieved from {}", classes, loader);

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

  @VisibleForTesting
  static void injectJarIntoContextClassLoader(URL url) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[] {url}, loader));
  }

  private void injectUrlIntoClassloader(URLClassLoader loader, URL url) {
    try {
      Method addUrl = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      addUrl.setAccessible(true);
      addUrl.invoke(loader, url);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private @Nullable ToolsClassLoader getToolsClassLoader() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    while (loader != null && !(loader instanceof ToolsClassLoader)) {
      loader = loader.getParent();
    }
    return (ToolsClassLoader) loader;
  }
}
