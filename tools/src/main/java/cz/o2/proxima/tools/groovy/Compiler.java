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
package cz.o2.proxima.tools.groovy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.Repository.Validate;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/** A compiler of conf files to groovy object. */
public class Compiler {

  private final Configuration conf = new Configuration(Configuration.VERSION_2_3_23);
  private final DefaultParser cli = new DefaultParser();

  private final String output;
  private final String packageName;
  private final List<String> configs;

  public static void main(String[] args) throws Exception {
    Compiler compiler = new Compiler(args);
    compiler.run();
  }

  @VisibleForTesting
  Compiler(String[] args) throws ParseException {
    Options opts = getOpts();
    CommandLine parsed = cli.parse(opts, args);

    if (!parsed.hasOption("o")) {
      throw new IllegalArgumentException("Missing config option 'o' for output");
    }

    packageName = parsed.hasOption("p") ? parsed.getOptionValue("p") : "";
    output = parsed.getOptionValue("o").replace("/", File.separator).trim();
    File outputFile = new File(output);
    Preconditions.checkArgument(!outputFile.isDirectory());
    configs =
        parsed
            .getArgList()
            .stream()
            .map(
                c -> {
                  File file = new File(c.trim());
                  if (!file.exists()) {
                    throw new IllegalArgumentException(
                        String.format(
                            "Unable to find config file '%s'. Please check parameters.", c.trim()));
                  }
                  return file.getPath();
                })
            .collect(Collectors.toList());

    if (configs.isEmpty()) {
      throw new IllegalArgumentException("Missing configuration files. Please check parameters.");
    }

    conf.setDefaultEncoding(StandardCharsets.UTF_8.name());
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);
  }

  public void run() throws Exception {
    Config config =
        configs
            .stream()
            .map(f -> ConfigFactory.parseFile(new File(f)))
            .reduce(ConfigFactory.empty(), Config::withFallback)
            .resolve();

    Repository repo =
        ConfigRepository.Builder.of(config)
            .withReadOnly(true)
            .withValidate(Validate.NONE)
            .withLoadFamilies(true)
            .build();

    String source = GroovyEnv.getSource(conf, repo, packageName);
    File of = new File(output);
    ensureParentDir(of);
    try (FileOutputStream fos = new FileOutputStream(of)) {
      fos.write(source.getBytes(StandardCharsets.UTF_8));
    }
  }

  private Options getOpts() {
    return new Options()
        .addOption(new Option("o", true, "Output filename"))
        .addOption(new Option("p", true, "Package name"));
  }

  private void ensureParentDir(File f) {
    File parent = f.getParentFile();
    if (!parent.exists()) {
      parent.mkdirs();
    } else if (!parent.isDirectory()) {
      throw new IllegalArgumentException(
          "Path " + parent.getAbsolutePath() + " exists and is not directory!");
    }
  }
}
