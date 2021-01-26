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
package cz.o2.proxima.generator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.Repository.Validate;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.util.CamelCase;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

/** Generates code for accessing data of entity and it's attributes. */
@Slf4j
public class ModelGenerator {

  /** Java package the model classes are going to be generated in. */
  private final String javaPackage;

  /** Class name of the generated model. */
  private final String className;

  /** Path of the config to generate the model from. */
  private final File configPath;

  /** Output path for the generated model. */
  private final File outputPath;

  /**
   * Construct the {@link ModelGenerator}.
   *
   * @param javaPackage Java package the model classes are going to be generated in.
   * @param className Class name of the generated model.
   * @param configPath Config to generate the model from.
   * @param outputPath Output directory for the generated model.
   */
  public ModelGenerator(String javaPackage, String className, File configPath, File outputPath) {
    this(javaPackage, className, configPath, outputPath, true);
  }

  ModelGenerator(
      String javaPackage, String className, File configPath, File outputPath, boolean validate) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(javaPackage), "Java package name is missing");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(className), "Class name is missing");
    this.javaPackage = javaPackage;
    this.className = className;
    this.configPath = configPath;
    this.outputPath = outputPath;
    if (validate) {
      if (!this.configPath.exists()) {
        throw new IllegalArgumentException(
            String.format("Source config not found at [%s]", configPath));
      }
      if (!this.outputPath.isAbsolute()) {
        throw new IllegalArgumentException(
            String.format("Output path must be absolute [%s]", outputPath));
      }
    }
  }

  public void generate() throws Exception {
    File output = getOutputDirForPackage(outputPath, javaPackage);
    final File outputFile = new File(output, className + ".java");
    if (!output.exists() && !output.mkdirs()) {
      throw new IllegalStateException(
          String.format("Failed to create directories for [%s]", outputPath.getAbsolutePath()));
    }

    try (FileOutputStream out = new FileOutputStream(outputFile)) {
      generate(ConfigFactory.parseFile(configPath).resolve(), new OutputStreamWriter(out));
    }
  }

  @VisibleForTesting
  void generate(Config config, Writer writer) throws IOException, TemplateException {

    final Configuration conf = getConf();

    final Repository repo =
        ConfigRepository.Builder.of(config)
            .withCachingEnabled(false)
            .withReadOnly(true)
            .withValidate(Validate.NONE)
            .withLoadFamilies(false)
            .withLoadClasses(false)
            .build();

    Map<String, Object> root = new HashMap<>();

    List<OperatorGenerator> operatorGenerators = getOperatorGenerators(repo);

    final Set<String> operatorImports =
        operatorGenerators
            .stream()
            .map(OperatorGenerator::imports)
            .reduce(Sets.newHashSet(), Sets::union);

    final List<Map<String, String>> operators =
        operatorGenerators.stream().map(this::toOperatorSubclassDef).collect(Collectors.toList());

    final List<Map<String, Object>> entities = getEntities(repo);

    root.put("input_path", configPath.getAbsoluteFile());
    root.put("input_config", readFileToString(configPath));
    root.put("java_package", javaPackage);
    root.put("java_classname", className);
    root.put("java_config_resourcename", configPath.getName());
    root.put("entities", entities);
    root.put("imports", operatorImports);
    root.put("operators", operators);
    Template template = conf.getTemplate("java-source.ftlh");
    template.process(root, writer);
  }

  private Configuration getConf() {
    Configuration conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);
    return conf;
  }

  private File getOutputDirForPackage(File outputPath, String javaPackage) {
    String packagePath = javaPackage.replace(".", File.separator);
    return new File(outputPath, packagePath);
  }

  private List<Map<String, Object>> getEntities(Repository repo) {
    List<Map<String, Object>> ret = new ArrayList<>();
    repo.getAllEntities().forEach(e -> ret.add(getEntityDict(e, repo)));
    return ret;
  }

  private Map<String, Object> getEntityDict(EntityDescriptor e, Repository repo) {
    Map<String, Object> ret = new HashMap<>();
    ret.put("classname", toClassName(e.getName()));
    ret.put("name", e.getName());
    ret.put("nameCamel", CamelCase.apply(e.getName()));

    List<Map<String, Object>> attributes =
        e.getAllAttributes()
            .stream()
            .map(
                attr -> {
                  ValueSerializerFactory serializerFactory =
                      repo.getValueSerializerFactory(attr.getSchemeUri().getScheme())
                          .orElseThrow(
                              () ->
                                  new IllegalStateException(
                                      String.format(
                                          "Unable to retrieve serializer factory for scheme %s. Looks like missing dependency for maven plugin.",
                                          attr.getSchemeUri().getScheme())));

                  Map<String, Object> attrMap = new HashMap<>();
                  String nameModified = attr.toAttributePrefix(false);
                  attrMap.put("wildcard", attr.isWildcard());
                  attrMap.put("nameRaw", attr.getName());
                  attrMap.put("name", nameModified);
                  attrMap.put(
                      "nameCamel",
                      CamelCase.apply(
                          nameModified, CamelCase.Characters.SPACE_DASH_AND_UNDERSCORE));
                  attrMap.put("nameUpper", nameModified.toUpperCase());
                  attrMap.put("type", serializerFactory.getClassName(attr.getSchemeUri()));
                  return attrMap;
                })
            .collect(Collectors.toList());
    ret.put("attributes", attributes);
    return ret;
  }

  private String toClassName(String name) {
    return CamelCase.apply(name);
  }

  private String readFileToString(File path) {
    try (FileInputStream in = new FileInputStream(path)) {
      return IOUtils.readLines(in, StandardCharsets.UTF_8)
          .stream()
          .map(s -> "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\\n\"")
          .collect(Collectors.joining("\n + "));
    } catch (IOException ex) {
      log.warn("Failed to read file {}. Ignoring.", path, ex);
      return "FAILED: " + ex.getMessage();
    }
  }

  private List<OperatorGenerator> getOperatorGenerators(Repository repo) {
    List<OperatorGenerator> ret = new ArrayList<>();
    ServiceLoader<OperatorGeneratorFactory> loader =
        ServiceLoader.load(OperatorGeneratorFactory.class);
    for (OperatorGeneratorFactory ogf : loader) {
      ret.add(ogf.create(repo));
    }
    return ret;
  }

  private Map<String, String> toOperatorSubclassDef(OperatorGenerator generator) {
    Map<String, String> ret = new HashMap<>();
    ret.put("operatorClass", generator.getOperatorClassName());
    ret.put("classdef", generator.classDef());
    ret.put("name", generator.operatorFactory().getOperatorName());
    ret.put("classname", toClassName(generator.operatorFactory().getOperatorName() + " operator"));
    return ret;
  }
}
