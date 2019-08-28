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
import cz.o2.proxima.util.Classpath;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import groovy.lang.Script;
import org.junit.Before;

/**
 * Base class for all tests compiling groovy source.
 */
public class GroovyTest {

  final Config cfg = ConfigFactory.load("test-reference.conf").resolve();
  final Repository repo = ConfigRepository.of(cfg);
  Configuration conf;
  ToolsClassLoader loader;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);

    loader = (ToolsClassLoader) Thread.currentThread().getContextClassLoader();
  }

  @SuppressWarnings(value = "unchecked")
  Script compile(String script) throws Exception {
    Class<Script> parsed = loader.parseClass(script);
    return Classpath.newInstance(parsed);
  }

}
