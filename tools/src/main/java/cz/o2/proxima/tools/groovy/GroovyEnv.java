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

import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import freemarker.template.Configuration;
import freemarker.template.Template;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Dynamic groovy descriptor of entity. */
@Slf4j
public class GroovyEnv {

  public static void createWrapperInLoader(
      Configuration conf, Repository repo, ToolsClassLoader loader) throws Exception {

    String source = getSource(conf, repo, "");
    loader.parseClass(source);
  }

  public static String getSource(Configuration conf, Repository repo) throws Exception {

    return getSource(conf, repo, "");
  }

  public static String getSource(Configuration conf, Repository repo, String packageName)
      throws Exception {

    Map<String, Object> root = new HashMap<>();
    List<Map<String, Object>> entities = new ArrayList<>();
    repo.getAllEntities()
        .forEach(
            entityDesc -> {
              Map<String, Object> entity = new HashMap<>();

              List<Map<String, Object>> attributes =
                  entityDesc
                      .getAllAttributes()
                      .stream()
                      .map(
                          a -> {
                            ValueSerializerFactory serializerFactory =
                                repo.getValueSerializerFactory(a.getSchemeUri().getScheme())
                                    .orElseThrow(
                                        () ->
                                            new IllegalStateException(
                                                "Unable to get ValueSerializerFactory for attribute "
                                                    + a.getName()
                                                    + " with scheme "
                                                    + a.getSchemeUri().toString()
                                                    + "."));

                            Map<String, Object> ret = new HashMap<>();
                            String name = a.toAttributePrefix(false);
                            ret.put("classname", toFirstUpper(name));
                            ret.put("type", serializerFactory.getClassName(a.getSchemeUri()));
                            ret.put("origname", a.getName());
                            ret.put("name", name);
                            ret.put("fieldname", name.toLowerCase());
                            ret.put("wildcard", a.isWildcard());
                            return ret;
                          })
                      .collect(Collectors.toList());

              entity.put("attributes", attributes);
              entity.put("classname", toFirstUpper(entityDesc.getName()));
              entity.put("name", entityDesc.getName());
              entities.add(entity);
            });

    root.put("entities", entities);
    root.put("groovyPackage", packageName);
    Template template = conf.getTemplate("class-entitydesc.ftlh");
    StringWriter writer = new StringWriter();
    template.process(root, writer);
    writer.flush();
    String ret = writer.toString();
    log.debug("Generated groovy source {}", ret);
    return ret;
  }

  private static Object toFirstUpper(String name) {
    if (name.isEmpty()) {
      return name;
    }
    char[] charArray = name.toCharArray();
    charArray[0] = Character.toUpperCase(charArray[0]);
    return new String(charArray);
  }

  private GroovyEnv() {
    // nop
  }
}
