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
package cz.o2.proxima.beam.util;

import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.util.ExceptionUtils;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.spark.SparkCommonPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;

@Internal
@Slf4j
public class RunnerUtils {

  /** Register given set of jars to runner. */
  public static void registerToPipeline(
      PipelineOptions opts, String runnerName, Collection<File> paths) {
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
                + "If you are experiencing issues with running and/or job submission, "
                + "please fill github issue reporting the name of the runner.",
            runnerName);
      }
      injectJarIntoContextClassLoader(paths);
    }
  }

  /** Inject given paths to class loader of given (local) runner. */
  public static void injectJarIntoContextClassLoader(Collection<File> paths) {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    URL[] urls =
        paths.stream()
            .map(p -> ExceptionUtils.uncheckedFactory(() -> p.toURI().toURL()))
            .collect(Collectors.toList())
            .toArray(new URL[] {});
    Thread.currentThread().setContextClassLoader(new URLClassLoader(urls, loader));
  }

  /**
   * Generate jar from provided map of dynamic classes.
   *
   * @param classes map of class to bytecode
   * @return generated {@link File}
   */
  public static File createJarFromDynamicClasses(Map<? extends Class<?>, byte[]> classes)
      throws IOException {
    Path tempFile = Files.createTempFile("proxima-beam-dynamic", ".jar");
    File out = tempFile.toFile();
    out.deleteOnExit();
    try (JarOutputStream output = new JarOutputStream(new FileOutputStream(out))) {
      long now = System.currentTimeMillis();
      for (Map.Entry<? extends Class<?>, byte[]> e : classes.entrySet()) {
        String name = e.getKey().getName().replace('.', '/') + ".class";
        JarEntry entry = new JarEntry(name);
        entry.setTime(now);
        output.putNextEntry(entry);
        InputStream input = new ByteArrayInputStream(e.getValue());
        IOUtils.copy(input, output);
        output.closeEntry();
      }
    }
    return out;
  }

  private static List<String> addToList(
      @Nonnull List<String> first, @Nullable List<String> second) {
    Collection<String> res = new HashSet<>(first);
    if (second != null) {
      res.addAll(second);
    }
    return new ArrayList<>(res);
  }

  private RunnerUtils() {}
}
