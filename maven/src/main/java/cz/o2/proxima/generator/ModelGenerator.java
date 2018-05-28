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
package cz.o2.proxima.generator;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.CamelCase;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Generates code for accessing data of entity and it's attributes.
 */
public class ModelGenerator {

  private final String javaPackage;
  private final String className;
  private final File sourceConfigPath;
  private final File outputPath;

  public ModelGenerator(
      String javaPackage, String className, String sourceConfigPath, String outputPath) {

    Preconditions.checkArgument(StringUtils.isNotBlank(javaPackage), "Java package name is missing");
    Preconditions.checkArgument(StringUtils.isNotBlank(className), "Class name is missing");

    this.javaPackage = javaPackage;
    this.className = className;
    this.sourceConfigPath = new File(sourceConfigPath);
    this.outputPath = new File(outputPath);

    if (!this.sourceConfigPath.exists()) {
      throw new IllegalArgumentException("Source config not found at [ " + sourceConfigPath + " ]");
    }

    if (!this.outputPath.isAbsolute()) {
      throw new IllegalArgumentException("Output path must be absolute [ " + outputPath + " ]");
    }
  }

  public void generate() throws Exception {

    Configuration conf = getConf();

    File output = getOutputDirForPackage(outputPath, javaPackage);
    if (!output.exists()) {
      if (!output.mkdirs()) {
        throw new RuntimeException(
            "Failed to create directories for [ " + output.getAbsolutePath() + " ]");
      }
    }

    final Repository repo = ConfigRepository.Builder
        .of(ConfigFactory.parseFile(sourceConfigPath).resolve())
        .withReadOnly(true)
        .withValidate(false)
        .withLoadFamilies(false)
        .withLoadAccessors(false)
        .build();


    Map<String, Object> root = new HashMap<>();

    List<Map<String, Object>> entities = getEntities(repo);
    try (FileOutputStream out = new FileOutputStream(new File(output, className + ".java"))) {
      root.put("input_path", sourceConfigPath.getAbsoluteFile());
      root.put("input_config", readFileToString(sourceConfigPath));
      root.put("java_package", javaPackage);
      root.put("java_classname", className);
      root.put("java_config_resourcename", sourceConfigPath.getName());
      root.put("entities", entities);
      Template template = conf.getTemplate("java-source.ftlh");
      template.process(root, new OutputStreamWriter(out));
    }
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
    String packagePath = javaPackage.replaceAll("\\.", File.separator);
    return new File(outputPath, packagePath);
  }

  private List<Map<String, Object>> getEntities(Repository repo) {
    List<Map<String, Object>> ret = new ArrayList<>();
    repo.getAllEntities().forEach(e -> ret.add(getEntityDict(e)));
    return ret;
  }

  private Map<String, Object> getEntityDict(EntityDescriptor e) {
    Map<String, Object> ret = new HashMap<>();
    ret.put("classname", toClassName(e.getName()));
    ret.put("name", e.getName());
    ret.put("nameCamel", CamelCase.apply(e.getName()));

    List<Map<String, Object>> attributes = e.getAllAttributes().stream()
        .map(attr -> {
          Map<String, Object> attrMap = new HashMap<>();
          String nameModified = attr.toAttributePrefix(false);
          attrMap.put("wildcard", attr.isWildcard());
          attrMap.put("nameRaw", attr.getName());
          attrMap.put("name", nameModified);
          attrMap.put("nameCamel", CamelCase.apply(nameModified));
          attrMap.put("nameUpper", nameModified.toUpperCase());
          // FIXME: this is working just for protobufs
          attrMap.put("type", attr.getSchemeURI().getSchemeSpecificPart());
          return attrMap;
        })
        .collect(Collectors.toList());
    ret.put("attributes", attributes);
    return ret;
  }

  private String toClassName(String name) {
    return CamelCase.apply(name);
  }

  private String readFileToString(File path) throws IOException {
    return Joiner.on("\n + ").join(
        IOUtils.readLines(new FileInputStream(path), "UTF-8")
            .stream().map(s -> "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\\n\"")
            .collect(Collectors.toList()));
  }
}
