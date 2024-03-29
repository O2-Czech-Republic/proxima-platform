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
package cz.o2.proxima.beam.tools.groovy;

import static cz.o2.proxima.beam.tools.groovy.BeamStream.dehydrate;

import com.google.api.client.util.Lists;
import com.google.auto.service.AutoService;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.storage.commitlog.Position;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.MoreObjects;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.tools.groovy.Stream;
import cz.o2.proxima.tools.groovy.StreamProvider;
import cz.o2.proxima.tools.groovy.ToolsClassLoader;
import cz.o2.proxima.tools.groovy.WindowedStream;
import groovy.lang.Closure;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

/** A {@link StreamProvider} for groovy tools based on beam. */
@Slf4j
public abstract class BeamStreamProvider implements StreamProvider {

  @FunctionalInterface
  public interface RunnerRegistrar {
    void apply(PipelineOptions opts);
  }

  @AutoService(StreamProvider.class)
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

  /**
   * Create a {@link Stream} from given {@link PCollection}.
   *
   * @param repo {@link Repository} to use
   * @param pCollection the {@link PCollection} to wrap
   * @return {@link PCollection} wrapped as {@link Stream}.
   */
  public static <T> Stream<T> wrap(Repository repo, PCollection<T> pCollection) {
    return BeamStream.wrap(repo, pCollection);
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
  public <T> WindowedStream<T> impulse(String name, Closure<T> factory) {
    Closure<T> dehydrated = dehydrate(factory);
    return BeamStream.impulse(name, beam, getJarRegisteringPipelineFactory(), dehydrated::call);
  }

  @Override
  public <T> WindowedStream<T> periodicImpulse(String name, Closure<T> factory, long durationMs) {
    Closure<T> dehydrated = dehydrate(factory);
    return BeamStream.periodicImpulse(
        name, beam, getJarRegisteringPipelineFactory(), dehydrated::call, durationMs);
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
  @VisibleForTesting
  void createUdfJarAndRegisterToPipeline(PipelineOptions opts) {
    String runnerName = opts.getRunner().getSimpleName();
    try {
      File path = createJarFromUdfs();
      log.info("Created jar {} with generated classes.", path);
      List<File> files = new ArrayList<>(Collections.singletonList(path));
      getAddedJars().stream().map(u -> new File(u.getPath())).forEach(files::add);
      registerToPipeline(opts, runnerName, files);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private Collection<URI> getAddedJars() {
    return Optional.ofNullable(getToolsClassLoader())
        .map(ToolsClassLoader::getAddedURLs)
        .orElse(Collections.emptySet());
  }

  private void registerToPipeline(PipelineOptions opts, String runnerName, Collection<File> paths) {
    log.info("Adding jars {} into classpath for runner {}", paths, runnerName);
    List<String> filesToStage =
        paths.stream().map(File::getAbsolutePath).collect(Collectors.toList());
    if (runnerName.equals("FlinkRunner")) {
      FlinkPipelineOptions flinkOpts = opts.as(FlinkPipelineOptions.class);
      flinkOpts.setFilesToStage(addToList(filesToStage, flinkOpts.getFilesToStage()));
    } else if (runnerName.equals("SparkRunner")) {
      SparkCommonPipelineOptions sparkOpts = opts.as(SparkCommonPipelineOptions.class);
      sparkOpts.setFilesToStage(addToList(filesToStage, sparkOpts.getFilesToStage()));
    } else {
      if (!runnerName.equals("DirectRunner")) {
        log.warn(
            "Injecting jar into unknown runner {}. It might not work as expected. "
                + "If you are experiencing issues with sub run and/or submission, "
                + "please fill github issue reporting the name of the runner.",
            runnerName);
      }
      injectJarIntoContextClassLoader(paths);
    }
  }

  private List<String> addToList(@Nonnull List<String> first, @Nullable List<String> second) {
    Collection<String> res = new HashSet<>(first);
    if (second != null) {
      res.addAll(second);
    }
    return new ArrayList<>(res);
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
  static void injectJarIntoContextClassLoader(Collection<File> paths) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL[] urls =
        paths.stream()
            .map(p -> ExceptionUtils.uncheckedFactory(() -> p.toURI().toURL()))
            .collect(Collectors.toList())
            .toArray(new URL[] {});
    Thread.currentThread().setContextClassLoader(new URLClassLoader(urls, loader));
  }

  private @Nullable ToolsClassLoader getToolsClassLoader() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    while (loader != null && !(loader instanceof ToolsClassLoader)) {
      loader = loader.getParent();
    }
    return (ToolsClassLoader) loader;
  }
}
