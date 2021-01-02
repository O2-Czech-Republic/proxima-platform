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
package cz.o2.proxima.maven;

import cz.o2.proxima.generator.ModelGenerator;
import java.io.File;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/** A MOJO for compiling config file to java classes. */
@Mojo(name = "compile", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class CompileMojo extends AbstractMojo {

  @Parameter private String outputDir;

  @Parameter private String config;

  @Parameter private String javaPackage;

  @Parameter private String className = "Model";

  @Parameter(defaultValue = "${project}")
  private MavenProject project;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {

    validate();

    File targetOutputDir = new File(outputDir);
    if (!targetOutputDir.isAbsolute()) {
      targetOutputDir = new File(new File(project.getBasedir(), "target"), outputDir);
    }

    ModelGenerator generator =
        new ModelGenerator(
            javaPackage, className, new File(config), new File(targetOutputDir.getAbsolutePath()));

    try {
      generator.generate();
      project.addCompileSourceRoot(targetOutputDir.getCanonicalPath());
    } catch (Exception ex) {
      throw new MojoExecutionException("Cannot create output", ex);
    }
  }

  private void validate() throws MojoFailureException {
    if (outputDir == null || config == null || javaPackage == null) {
      throw new MojoFailureException(
          "Missing required parameter `outputDir', " + "`config' or `javaPackage'");
    }
  }
}
