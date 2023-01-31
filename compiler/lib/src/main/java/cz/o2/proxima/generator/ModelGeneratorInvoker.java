/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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

import java.io.File;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

class ModelGeneratorInvoker {

  static ModelGenerator of(String[] args) {
    return new ModelGeneratorInvoker(args).create();
  }

  private final String[] args;

  ModelGeneratorInvoker(String[] args) {
    this.args = args;
  }

  private Options getOpts() {
    Options opts = new Options();
    opts.addOption("c", "class", true, "Name of generated class");
    opts.addOption("t", "target", true, "Target directory");
    opts.addRequiredOption("p", "package", true, "Package of the generated class");
    opts.addRequiredOption("f", "file", true, "Path to the configuration file");
    return opts;
  }

  private ModelGenerator create() {
    Options opts = getOpts();
    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine parsed = parser.parse(opts, args);
      String packageName = parsed.getOptionValue('p');
      String className = parsed.getOptionValue('c', "Model");
      String targetDirectory = parsed.getOptionValue('t', "./");
      String file = parsed.getOptionValue('f');
      return new ModelGenerator(
          packageName, className, new File(file), new File(targetDirectory).getAbsoluteFile());
    } catch (ParseException ex) {
      System.err.println(ex.getMessage());
      new HelpFormatter().printHelp("Usage:", opts);
      return null;
    }
  }
}
