/*
 * Copyright 2017-2026 O2 Czech Republic, a.s.
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Class for running scripts written in groovy.
 *
 * <p>Usage: #!/opt/proxima/bin/proxima
 *
 * <p>rules = env.gateway.rule.streamFromOldest() ...
 */
public class ProximaShell extends ShellRunnable {

  public ProximaShell(Repository repo, String[] args) {
    super(repo, args);
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      usage();
    }
    String input = args[0];
    ProximaShell shell =
        new ProximaShell(
            Repository.of(ConfigFactory.load().resolve()),
            Arrays.copyOfRange(args, 1, args.length));
    shell.run(new File(input));
  }

  @VisibleForTesting
  Object run(File inputFile) throws Exception {
    Preconditions.checkArgument(inputFile.exists(), "File %s does not exist", inputFile);
    initializeStreamProvider();
    updateClassLoader();
    Class<? extends RepositoryProvider> wrapperClass = createWrapperClass();
    Binding binding = new Binding();
    binding.setVariable("env", Classpath.newInstance(wrapperClass));
    GroovyShell shell = new GroovyShell(binding);
    List<String> lines = Files.readAllLines(inputFile.toPath());
    String code = Joiner.on("\n").join(Iterables.filter(lines, line -> !line.startsWith("#")));
    Object result = shell.run(code, inputFile.getName(), Collections.emptyList());
    System.out.println(result.toString());
    return result;
  }

  private static void usage() {
    System.err.printf(
        "Usage: %s <input_file> [<runner_specific_args>]\n", ProximaShell.class.getSimpleName());
  }
}
