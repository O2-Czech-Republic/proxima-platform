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
package cz.o2.proxima.direct.generator;

import com.google.common.collect.Sets;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.DirectAttributeFamilyDescriptor;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.DirectDataOperatorFactory;
import cz.o2.proxima.direct.randomaccess.KeyValue;
import cz.o2.proxima.direct.randomaccess.RandomAccessReader;
import cz.o2.proxima.generator.OperatorGenerator;
import cz.o2.proxima.generator.OperatorGeneratorFactory;
import cz.o2.proxima.repository.DataOperatorFactory;
import cz.o2.proxima.repository.Repository;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Set;

/** {@link OperatorGeneratorFactory} for {@link DirectDataOperator}. */
public class DirectOperatorGeneratorFactory implements OperatorGeneratorFactory {

  static class Generator implements OperatorGenerator {

    final Repository repo;
    final Configuration conf = getConf();

    Generator(Repository repo) {
      this.repo = repo;
    }

    @Override
    public DataOperatorFactory<?> operatorFactory() {
      return new DirectDataOperatorFactory();
    }

    @Override
    public Set<String> imports() {
      return Sets.newHashSet(
          DirectDataOperator.class.getName(),
          DirectAttributeFamilyDescriptor.class.getName(),
          CommitLogReader.class.getName(),
          KeyValue.class.getName(),
          RandomAccessReader.class.getName());
    }

    @Override
    public String classDef() {
      try {
        Template template = conf.getTemplate("direct-java-source.ftlh");
        try (StringWriter writer = new StringWriter()) {
          template.process(new HashMap<>(), writer);
          return writer.toString();
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    private Configuration getConf() {
      Configuration freeMarkerConf = new Configuration(Configuration.VERSION_2_3_23);
      freeMarkerConf.setDefaultEncoding("utf-8");
      freeMarkerConf.setClassForTemplateLoading(getClass(), "/");
      freeMarkerConf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      freeMarkerConf.setLogTemplateExceptions(false);
      return freeMarkerConf;
    }

    @Override
    public String getOperatorClassName() {
      return DirectDataOperator.class.getName();
    }
  }

  @Override
  public OperatorGenerator create(Repository repo) {
    return new Generator(repo);
  }
}
