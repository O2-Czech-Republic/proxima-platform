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
package cz.o2.proxima.tools.groovy;

import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.util.Classpath;
import cz.o2.proxima.typesafe.config.Config;
import cz.o2.proxima.typesafe.config.ConfigFactory;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import groovy.lang.Script;
import org.junit.After;
import org.junit.Before;

/** Base class for all tests compiling groovy source. */
public class GroovyTest {

  final Config cfg = ConfigFactory.load("test-reference.conf").resolve();
  final Repository repo = Repository.ofTest(cfg);
  Configuration conf;
  ToolsClassLoader loader;
  Console console;

  @Before
  public void setUp() {
    conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);
    console = Console.create(cfg, repo);

    loader = (ToolsClassLoader) Thread.currentThread().getContextClassLoader();
  }

  @After
  public void tearDown() {
    console.close();
  }

  @SuppressWarnings(value = "unchecked")
  Script compile(String script) throws Exception {
    Class<Script> parsed = loader.parseClass(script);
    return Classpath.newInstance(parsed);
  }
}
