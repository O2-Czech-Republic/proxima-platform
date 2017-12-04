/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.seznam.euphoria.shaded.guava.com.google.common.base.Joiner;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * A MOJO for compiling config file to java classes.
 */
@Mojo(name = "compile")
public class CompileMojo extends AbstractMojo {

  @Parameter
  private String outputDir;

  @Parameter
  private String config;

  @Parameter
  private String javaPackage;

  @Parameter
  private String className = "Model";

  @Parameter(defaultValue = "${project}")
  private MavenProject project;

  private Configuration conf;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {

    validate();

    conf = getConf();

    File output = toOutputDir(javaPackage, outputDir);
    if (!output.exists()) {
      output.mkdirs();
    }
    Repository repo = Repository.Builder
        .of(ConfigFactory.parseFile(new File(config)).resolve())
        .withReadOnly(true)
        .withValidate(false)
        .withLoadFamilies(false)
        .withLoadAccessors(false)
        .build();
    Map<String, Object> root = new HashMap<>();

    List<Map<String, Object>> entities = getEntities(repo);
    try (FileOutputStream out = new FileOutputStream(new File(output, className + ".java"))) {
      root.put("input_path", config);
      root.put("input_config", readFileToString(config));
      root.put("java_package", javaPackage);
      root.put("java_classname", className);
      root.put("java_config_resourcename", new File(config).getName());
      root.put("entities", entities);
      Template template = conf.getTemplate("java-source.ftlh");
      template.process(root, new OutputStreamWriter(out));
      project.addCompileSourceRoot(output.getCanonicalPath());
    } catch (Exception ex) {
      throw new MojoExecutionException("Cannot create output", ex);
    }
  }

  private void validate() throws MojoFailureException {
    if (outputDir == null || config == null || javaPackage == null) {
      throw new MojoFailureException("Missing required parameter `outputDir', "
          + "`config' or `javaPackage'");
    }
  }

  private List<Map<String, Object>> getEntities(Repository repo) {
    List<Map<String, Object>> ret = new ArrayList<>();
    repo.getAllEntities().forEach(e -> {
      ret.add(getEntityDict(e));
    });
    return ret;
  }

  private Configuration getConf() {
    conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);
    return conf;
  }

  private Map<String, Object> getEntityDict(EntityDescriptor e) {
    Map<String, Object> ret = new HashMap<>();
    ret.put("classname", toClassName(e.getName()));
    ret.put("name", e.getName());
    ret.put("nameCamel", toCamelCase(e.getName()));

    List<Map<String, Object>> attributes = e.getAllAttributes().stream()
        .map(attr -> {
          Map<String, Object> attrMap = new HashMap<>();
          String nameModified = attr.toAttributePrefix(false);
          attrMap.put("wildcard", attr.isWildcard());
          attrMap.put("nameRaw", attr.getName());
          attrMap.put("name", nameModified);
          attrMap.put("nameCamel", toCamelCase(nameModified));
          attrMap.put("nameUpper", nameModified.toUpperCase());
          // FIXME: this is working just for protobufs
          attrMap.put("type", attr.getSchemeURI().getSchemeSpecificPart());
          return attrMap;
        })
        .collect(Collectors.toList());
    ret.put("attributes", attributes);
    return ret;
  }

  private String toCamelCase(String what) {
    if (what.isEmpty()) {
      throw new IllegalArgumentException("Entity name cannot be empty.");
    }
    char[] chars = what.toCharArray();
    chars[0] = Character.toUpperCase(chars[0]);
    return new String(chars);
  }

  private String toClassName(String name) {
     return toCamelCase(name);
  }

  private File toOutputDir(String javaPackage, String outputDir) {
    File target = new File(project.getBasedir(), "target");
    String replaced = javaPackage.replaceAll("\\.", File.separator);
    return new File(new File(target, outputDir), replaced);
  }

  private String readFileToString(String path)
      throws FileNotFoundException, IOException {

    return Joiner.on("\n + ").join(
        IOUtils.readLines(new FileInputStream(new File(path)))
            .stream().map(s -> "\"" + s.replace("\"", "\\\"") + "\\n\"")
            .collect(Collectors.toList()));
  }

}
