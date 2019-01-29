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
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test suite for {@link GroovyEnv}.
 */
@Ignore("Enable and test against beam")
public class GroovyEnvTest {

  final Config cfg = ConfigFactory.load("test-reference.conf").resolve();
  final Repository repo = ConfigRepository.of(cfg);
  final EntityDescriptor gateway = repo.findEntity("gateway")
      .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
  final EntityDescriptor batch = repo.findEntity("batch")
      .orElseThrow(() -> new IllegalStateException("Missing entity batch"));

  @SuppressWarnings("unchecked")
  final AttributeDescriptor<byte[]> armed = (AttributeDescriptor) gateway
      .findAttribute("armed")
      .orElseThrow(() -> new IllegalStateException("Missing attribute armed"));
  @SuppressWarnings("unchecked")
  final AttributeDescriptor<byte[]> device = (AttributeDescriptor) gateway
      .findAttribute("device.*")
      .orElseThrow(() -> new IllegalStateException("Missing attribute device"));
  @SuppressWarnings("unchecked")
  final AttributeDescriptor<byte[]> data = (AttributeDescriptor) batch
      .findAttribute("data")
      .orElseThrow(() -> new IllegalStateException("Missing attribute data"));
  @SuppressWarnings("unchecked")
  final AttributeDescriptor<byte[]> wildcard = (AttributeDescriptor) batch
      .findAttribute("wildcard.*")
      .orElseThrow(() -> new IllegalStateException("Missing attribute wildcard"));

  Configuration conf;

  GroovyClassLoader loader;

  DirectDataOperator direct;

  @Before
  public void setUp() {
    Console console = Console.create(cfg, repo);
    direct = console.getDirect().orElseThrow(
        () -> new IllegalStateException("Missing direct operator"));
    conf = new Configuration(Configuration.VERSION_2_3_23);
    conf.setDefaultEncoding("utf-8");
    conf.setClassForTemplateLoading(getClass(), "/");
    conf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    conf.setLogTemplateExceptions(false);

    loader = new GroovyClassLoader(Thread.currentThread().getContextClassLoader());
    Thread.currentThread().setContextClassLoader(loader);
  }

  @SuppressWarnings("unchecked")
  Script compile(String script) throws Exception {
    String source = GroovyEnv.getSource(conf, repo)
        + "\n"
        + "env = cz.o2.proxima.tools.groovy.Console.get().getEnv()"
        + "\n"
        + script;
    Class<Script> parsed = loader.parseClass(source);
    return parsed.newInstance();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStreamFromOldestCollect() throws Exception {
    Script compiled = compile("env.gateway.armed.streamFromOldest().collect()");
    direct.getWriter(armed)
        .orElseThrow(() -> new IllegalStateException("Missing writer"))
        .write(StreamElement.update(gateway, armed, "uuid",
            "key", armed.getName(), System.currentTimeMillis(), new byte[] { }),
            (succ, exc) -> { });
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUnionFromOldestCollect() throws Exception {
    Script compiled = compile("env.unionStreamFromOldest(env.gateway.armed).collect()");
    direct.getWriter(armed)
        .orElseThrow(() -> new IllegalStateException("Missing writer"))
        .write(StreamElement.update(gateway, armed, "uuid",
            "key", armed.getName(), System.currentTimeMillis(), new byte[] { }),
            (succ, exc) -> { });
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBatchUpdatesCollect() throws Exception {
    Script compiled = compile("env.batch.data.batchUpdates().collect()");
    direct.getWriter(armed)
        .orElseThrow(() -> new IllegalStateException("Missing writer"))
        .write(StreamElement.update(batch, data, "uuid",
            "key", data.getName(), System.currentTimeMillis(), new byte[] { }),
            (succ, exc) -> { });
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBatchUpdatesCollectWildcard() throws Exception {
    Script compiled = compile("env.batch.wildcard.batchUpdates().collect()");
    direct.getWriter(wildcard)
        .orElseThrow(() -> new IllegalStateException("Missing writer"))
        .write(StreamElement.update(batch, wildcard, "uuid",
            "key", wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(), new byte[] { }),
            (succ, exc) -> { });
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUnionBatchUpdatesCollect() throws Exception {
    Script compiled = compile(
        "env.unionBatchUpdates(env.batch.data, env.batch.wildcard).collect()");
    direct.getWriter(data)
        .orElseThrow(() -> new IllegalStateException("Missing writer"))
        .write(StreamElement.update(batch, data, "uuid",
            "key", data.getName(), System.currentTimeMillis(), new byte[] { }),
            (succ, exc) -> { });
    direct.getWriter(wildcard)
        .orElseThrow(() -> new IllegalStateException("Missing writer"))
        .write(StreamElement.update(batch, wildcard, "uuid",
            "key", wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(), new byte[] { }),
            (succ, exc) -> { });
    List<StreamElement> result = (List) compiled.run();
    assertEquals(2, result.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStreamFromOldestWindowedCollect() throws Exception {
    Script compiled = compile("env.gateway.armed.streamFromOldest()"
        + ".reduceToLatest().collect()");
    direct.getWriter(armed)
        .orElseThrow(() -> new IllegalStateException("Missing writer"))
        .write(StreamElement.update(gateway, armed, "uuid",
            "key", armed.getName(), System.currentTimeMillis(), new byte[] { }),
            (succ, exc) -> { });
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testStreamPersist() throws Exception {
    Script compiled = compile(
        "env.batch.data.batchUpdates().persist(env, env.gateway.desc(), { it.key }, "
            + "{ 'armed' }, { it.parsed.get() }, { it.stamp })\n"
        + "env.gateway.armed.streamFromOldest().collect()");
    direct.getWriter(data)
        .orElseThrow(() -> new IllegalStateException("Missing writer"))
        .write(StreamElement.update(batch, data, "uuid",
            "key", data.getName(), System.currentTimeMillis(), new byte[] { }),
            (succ, exc) -> { });

    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }


  @SuppressWarnings("unchecked")
  @Ignore(
      "This has to be implemented, reduceToLatest must take wildcard deletes "
          + "into account! "
          + "See https://github.com/O2-Czech-Republic/proxima-platform/issues/110")
  @Test
  public void testWildcardDelete() throws Exception {
    long now = 123456789000L;
    Script compiled = compile(
        "env.gateway.device.deleteAll(\"gw\", 1234567890000)\n"
        + "env.gateway.device.streamFromOldest().reduceToLatest().collect()");
    OnlineAttributeWriter writer = direct.getWriter(device)
        .orElseThrow(() -> new IllegalStateException("Missing writer"));
    writer.write(StreamElement.update(
        gateway, device, "uuid", "key", device.toAttributePrefix() + "1", now - 1,
        new byte[] { }), (succ, exc) -> { });
    writer.write(StreamElement.update(
        gateway, device, "uuid", "key", device.toAttributePrefix() + "2", now + 1,
        new byte[] { }), (succ, exc) -> { });
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWildcardDeleteRandomRead() throws Exception {
    long now = 123456789000L;
    Script compiled = compile(
        /* "env.gateway.device.deleteAll(\"gw\", 1234567890000)\n" */ ""
        + "env.gateway.device.list(\"gw\")");
    OnlineAttributeWriter writer = direct.getWriter(device)
        .orElseThrow(() -> new IllegalStateException("Missing writer"));
    writer.write(StreamElement.update(
        gateway, device, "uuid", "gw", device.toAttributePrefix() + "1", now - 1,
        new byte[] { }), (succ, exc) -> { });
    writer.write(StreamElement.update(
        gateway, device, "uuid", "key", device.toAttributePrefix() + "2", now + 1,
        new byte[] { }), (succ, exc) -> { });
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

}
