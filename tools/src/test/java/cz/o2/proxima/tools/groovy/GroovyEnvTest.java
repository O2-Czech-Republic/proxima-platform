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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import groovy.lang.Script;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test suite for {@link GroovyEnv}.
 */
public abstract class GroovyEnvTest extends GroovyTest {

  final EntityDescriptor gateway = repo.findEntity("gateway")
      .orElseThrow(() -> new IllegalStateException("Missing entity gateway"));
  final EntityDescriptor batch = repo.findEntity("batch")
      .orElseThrow(() -> new IllegalStateException("Missing entity batch"));
  final Console console = Console.create(cfg, repo);

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

  @Override
  Script compile(String script) throws Exception {
    String source = GroovyEnv.getSource(conf, repo) + "\n"
        + "env = cz.o2.proxima.tools.groovy.Console.get().getEnv()" + "\n" + script;
    return super.compile(source);
  }

  @Test
  public void testStreamFromOldestCollect() throws Exception {
    Script compiled = compile("env.gateway.armed.streamFromOldest().collect()");
    write(StreamElement.update(gateway, armed, "uuid",
        "key", armed.getName(), System.currentTimeMillis(), new byte[] { }));
    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testUnionFromOldestCollect() throws Exception {
    Script compiled = compile("env.unionStreamFromOldest(env.gateway.armed).collect()");
    write(StreamElement.update(gateway, armed, "uuid",
        "key", armed.getName(), System.currentTimeMillis(), new byte[] { }));
    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testBatchUpdatesCollect() throws Exception {
    Script compiled = compile("env.batch.data.batchUpdates().collect()");
    write(StreamElement.update(batch, data, "uuid",
        "key", data.getName(), System.currentTimeMillis(), new byte[] { }));
    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testBatchUpdatesCollectWildcard() throws Exception {
    Script compiled = compile("env.batch.wildcard.batchUpdates().collect()");
    write(StreamElement.update(batch, wildcard, "uuid",
        "key", wildcard.toAttributePrefix() + "1",
        System.currentTimeMillis(), new byte[] { }));
    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testUnionBatchUpdatesCollect() throws Exception {
    Script compiled = compile(
        "env.unionBatchUpdates(env.batch.data, env.batch.wildcard).collect()");
    write(StreamElement.update(batch, data, "uuid",
        "key", data.getName(), System.currentTimeMillis(), new byte[] { }));
    write(StreamElement.update(batch, wildcard, "uuid",
        "key", wildcard.toAttributePrefix() + "1",
        System.currentTimeMillis(), new byte[] { }));
    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(2, result.size());
  }

  @Test
  public void testStreamFromOldestWindowedCollect() throws Exception {
    Script compiled = compile("env.gateway.armed.streamFromOldest()"
        + ".reduceToLatest().collect()");
    write(StreamElement.update(gateway, armed, "uuid",
            "key", armed.getName(), System.currentTimeMillis(), new byte[] { }));
    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testStreamPersist() throws Exception {
    Script compiled = compile(
        "env.batch.data.batchUpdates().persist(env, env.gateway.desc, { it.key }, "
            + "{ 'armed' }, { it.parsed.get() }, { it.stamp })\n"
        + "env.gateway.armed.streamFromOldest().collect()");

    write(StreamElement.update(batch, data, "uuid",
        "key", data.getName(), System.currentTimeMillis(), new byte[] { }));

    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }


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
    write(StreamElement.update(
        gateway, device, "uuid", "key", device.toAttributePrefix() + "1", now - 1,
        new byte[] { }));
    write(StreamElement.update(
        gateway, device, "uuid", "key", device.toAttributePrefix() + "2", now + 1,
        new byte[] { }));
    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testWildcardDeleteRandomRead() throws Exception {
    long now = 123456789000L;
    Script compiled = compile(
        /* "env.gateway.device.deleteAll(\"gw\", 1234567890000)\n" */ ""
        + "env.gateway.device.list(\"gw\")");
    write(StreamElement.update(
        gateway, device, "uuid", "gw", device.toAttributePrefix() + "1", now - 1,
        new byte[] { }));
    write(StreamElement.update(
        gateway, device, "uuid", "key", device.toAttributePrefix() + "2", now + 1,
        new byte[] { }));
    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testMap() throws Exception {
    Script compiled = compile(
        "env.batch.data.batchUpdates().map({ \"\" }).collect()");

    write(StreamElement.update(batch, data, "uuid",
            "key", data.getName(), System.currentTimeMillis(), new byte[] { }));

    @SuppressWarnings("unchecked")
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testPrintln() throws Exception {
    Script compiled = compile(
        "env.batch.data.batchUpdates().forEach({ println it })");
    write(StreamElement.update(batch, data, "uuid",
            "key", data.getName(), System.currentTimeMillis(), new byte[] { }));
    compiled.run();
  }


  protected abstract void write(StreamElement element);

  protected Repository getRepo() {
    return repo;
  }

}
