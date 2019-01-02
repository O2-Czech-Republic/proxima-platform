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
package cz.o2.proxima.tools.groovy;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Repository;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import java.io.File;
import java.io.FileOutputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;

/**
 * A compiler of conf files to groovy object.
 */
public class Compiler {

  private static final Charset UTF8 = Charset.forName("UTF-8");
  private final Configuration conf = new Configuration(Configuration.VERSION_2_3_23);
  private final DefaultParser cli = new DefaultParser();

  private final String output;
  private final List<String> configs;

  public static void main(String[] args) throws Exception {
    Compiler compiler = new Compiler(args);
    compiler.run();
  }

  private Compiler(String[] args) throws ParseException {
    Options opts = getOpts();
    CommandLine parsed = cli.parse(opts, args);

    if (!parsed.hasOption("o")) {
      throw new IllegalStateException("Missing config option 'o' for output");
    }
    output = parsed.getOptionValue("o");
    configs = parsed.getArgList();

    conf.setDefaultEncoding(UTF8.name());
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);
  }

  public void run() throws Exception {
    Config config = configs.stream()
        .map(f -> ConfigFactory.parseFile(new File(f)))
        .reduce(
            ConfigFactory.empty(),
            (l, r) -> l.withFallback(r))
        .resolve();

    Repository repo = ConfigRepository.Builder.of(config)
        .withReadOnly(true)
        .withValidate(false)
        .withLoadFamilies(true)
        .withLoadAccessors(false)
        .build();

    String source = GroovyEnv.getSource(conf, repo);
    File of = new File(output);
    ensureParentDir(of);
    try (FileOutputStream fos = new FileOutputStream(of)) {
      StringReader reader = new StringReader(source);
      IOUtils.copy(reader, fos, UTF8);
    }
  }

  private Options getOpts() {
    return new Options()
        .addOption(new Option("o", true, "Output filename"));
  }

  private void ensureParentDir(File f) {
    File parent = f.getParentFile();
    if (!parent.exists()) {
      parent.mkdirs();
    } else if (!parent.isDirectory()) {
      throw new IllegalArgumentException("Path "
          + parent.getAbsolutePath()
          + " exists and is not directory!");
    }
  }


}
