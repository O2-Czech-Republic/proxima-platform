/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import static org.junit.Assert.*;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Pair;
import groovy.lang.Script;
import groovy.lang.Tuple;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Ignore;
import org.junit.Test;

/** Test suite for {@link GroovyEnv}. */
public abstract class GroovyEnvTest extends GroovyTest {

  final EntityDescriptor gateway = repo.getEntity("gateway");
  final EntityDescriptor batch = repo.getEntity("batch");
  final EntityDescriptor dummy = repo.getEntity("dummy");

  final AttributeDescriptor<byte[]> armed = gateway.getAttribute("armed");

  final AttributeDescriptor<byte[]> device = gateway.getAttribute("device.*");

  final AttributeDescriptor<byte[]> data = batch.getAttribute("data");

  final AttributeDescriptor<byte[]> wildcard = batch.getAttribute("wildcard.*");

  @Override
  Script compile(String script) throws Exception {
    String source =
        GroovyEnv.getSource(conf, repo) + "\n" + Console.INITIAL_STATEMENT + "\n" + script;
    return super.compile(source);
  }

  @Test
  public void testStreamFromOldestCollect() throws Exception {
    Script compiled = compile("env.gateway.armed.streamFromOldest().collect()");
    write(
        StreamElement.upsert(
            gateway,
            armed,
            "uuid",
            "key",
            armed.getName(),
            System.currentTimeMillis(),
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testUnionFromOldestCollect() throws Exception {
    Script compiled = compile("env.unionStreamFromOldest(env.gateway.armed).collect()");
    write(
        StreamElement.upsert(
            gateway,
            armed,
            "uuid",
            "key",
            armed.getName(),
            System.currentTimeMillis(),
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testBatchUpdatesCollect() throws Exception {
    Script compiled = compile("env.batch.data.batchUpdates().collect()");
    write(
        StreamElement.upsert(
            batch, data, "uuid", "key", data.getName(), System.currentTimeMillis(), new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testBatchUpdatesCollectWildcard() throws Exception {
    Script compiled = compile("env.batch.wildcard.batchUpdates().collect()");
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testBatchUpdatesCollectWildcardMultiple() throws Exception {
    Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates().collect()\n"
                + "env.batch.wildcard.batchUpdates().collect()");
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testUnionBatchUpdatesCollect() throws Exception {
    Script compiled =
        compile("env.unionBatchUpdates(env.batch.data, env.batch.wildcard).collect()");
    write(
        StreamElement.upsert(
            batch, data, "uuid", "key", data.getName(), System.currentTimeMillis(), new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(2, result.size());
  }

  @Test
  public void testStreamFromOldestWindowedCollect() throws Exception {
    Script compiled =
        compile("env.gateway.armed.streamFromOldest()" + ".reduceToLatest().collect()");
    write(
        StreamElement.upsert(
            gateway,
            armed,
            "uuid",
            "key",
            armed.getName(),
            System.currentTimeMillis(),
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testStreamPersist() throws Exception {
    Script compiled =
        compile(
            "env.batch.data.batchUpdates().persist(env, env.gateway.desc, { it.key }, "
                + "{ 'armed' }, { it.parsed.get() }, { it.stamp })\n"
                + "env.gateway.armed.streamFromOldest().collect()");

    write(
        StreamElement.upsert(
            batch, data, "uuid", "key", data.getName(), System.currentTimeMillis(), new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testPersistIntoTargetFamily() throws Exception {
    Script compiled =
        compile(
            "env.batch.data.batchUpdates().persistIntoTargetFamily(env, \"dummy-storage\")\n"
                + "env.dummy.data.streamFromOldest().collect()");

    write(
        StreamElement.upsert(
            batch, data, "uuid", "key", data.getName(), System.currentTimeMillis(), new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
    assertEquals(dummy, result.get(0).getEntityDescriptor());
  }

  @Test
  public void testPersistIntoTargetFamilyBulk() throws Exception {
    Script compiled =
        compile(
            "env.dummy.data.streamFromOldest().persistIntoTargetFamily(env, \"dummy-storage-bulk\")\n"
                + "env.dummy.data.batchUpdates().collect()");

    AttributeDescriptor<Object> dummyData = dummy.getAttribute("data");
    write(
        StreamElement.upsert(
            dummy,
            dummyData,
            "uuid",
            "key",
            dummyData.getName(),
            System.currentTimeMillis(),
            new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testClosureByteCodeAvailability() throws Exception {
    Script compiled = compile("def a = { it }");
    compiled.run();
    List<String> closures =
        loader
            .getDefinedClasses()
            .stream()
            .filter(n -> n.contains(("_run_closure")))
            .collect(Collectors.toList());
    List<byte[]> codes =
        closures.stream().map(loader::getClassByteCode).collect(Collectors.toList());
    assertEquals(closures.size(), codes.size());
  }

  @Ignore(
      "This has to be implemented, reduceToLatest must take wildcard deletes "
          + "into account! "
          + "See https://github.com/O2-Czech-Republic/proxima-platform/issues/110")
  @Test
  public void testWildcardDelete() throws Exception {
    long now = 123456789000L;
    Script compiled =
        compile(
            "env.gateway.device.deleteAll(\"gw\", 1234567890000)\n"
                + "env.gateway.device.streamFromOldest().reduceToLatest().collect()");
    write(
        StreamElement.upsert(
            gateway,
            device,
            "uuid",
            "key",
            device.toAttributePrefix() + "1",
            now - 1,
            new byte[] {}));
    write(
        StreamElement.upsert(
            gateway,
            device,
            "uuid",
            "key",
            device.toAttributePrefix() + "2",
            now + 1,
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testWildcardDeleteRandomRead() throws Exception {
    long now = 123456789000L;
    Script compiled =
        compile(
            /* "env.gateway.device.deleteAll(\"gw\", 1234567890000)\n" */ ""
                + "env.gateway.device.list(\"gw\")");
    write(
        StreamElement.upsert(
            gateway,
            device,
            "uuid",
            "gw",
            device.toAttributePrefix() + "1",
            now - 1,
            new byte[] {}));
    write(
        StreamElement.upsert(
            gateway,
            device,
            "uuid",
            "key",
            device.toAttributePrefix() + "2",
            now + 1,
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testMap() throws Exception {
    Script compiled = compile("env.batch.data.batchUpdates().map({ \"\" }).collect()");

    write(
        StreamElement.upsert(
            batch, data, "uuid", "key", data.getName(), System.currentTimeMillis(), new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<StreamElement> result = (List) compiled.run();
    assertEquals(1, result.size());
  }

  @Test
  public void testFlatMap() throws Exception {
    Script compiled =
        compile("env.batch.data.batchUpdates().flatMap({ [it.key, it.attribute] }).collect()");

    write(
        StreamElement.upsert(
            batch, data, "uuid", "key", data.getName(), System.currentTimeMillis(), new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<String> result = (List) compiled.run();
    assertEquals(Arrays.asList("key", data.getName()), result);
  }

  @Test
  public void testPrintln() throws Exception {
    Script compiled = compile("env.batch.data.batchUpdates().print()");
    write(
        StreamElement.upsert(
            batch, data, "uuid", "key", data.getName(), System.currentTimeMillis(), new byte[] {}));
    compiled.run();
    // make sonar happy
    assertTrue(true);
  }

  @Test
  public void testGroupReduce() throws Exception {
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates().groupReduce("
                + "{ it.key }, { w, el -> [[w.toString(), el.size()]] }).collect()");

    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid1",
            "key1",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key2",
            wildcard.toAttributePrefix() + "2",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid3",
            "key1",
            wildcard.toAttributePrefix() + "3",
            System.currentTimeMillis(),
            new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Pair<Object, List<Object>>> result = (List) compiled.run();
    Map<Object, List<Object>> resultMap =
        result.stream().collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    assertEquals(2, resultMap.get("key1").get(1));
    assertEquals(1, resultMap.get("key2").get(1));
  }

  @Test
  public void testJoin() throws Exception {
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates().join("
                + "env.batch.wildcard.batchUpdates(), { it.key }, { it.key })"
                + ".map({ new Tuple(it.first.key, it.second.key) })"
                + ".collect()");

    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid1",
            "key1",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key2",
            wildcard.toAttributePrefix() + "2",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid3",
            "key3",
            wildcard.toAttributePrefix() + "3",
            System.currentTimeMillis(),
            new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Tuple> result = (List) compiled.run();
    Map<String, String> resultMap =
        result
            .stream()
            .map(t -> Pair.of(t.get(0).toString(), t.get(1).toString()))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    assertEquals(3, resultMap.size());
    assertEquals("key1", resultMap.get("key1"));
    assertEquals("key2", resultMap.get("key2"));
    assertEquals("key3", resultMap.get("key3"));
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates().leftJoin("
                + "env.batch.wildcard.batchUpdates().filter({ it.key != \"key1\" }), { it.key }, { it.key })"
                + ".collect()");

    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid1",
            "key1",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key2",
            wildcard.toAttributePrefix() + "2",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid3",
            "key3",
            wildcard.toAttributePrefix() + "3",
            System.currentTimeMillis(),
            new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Pair<StreamElement, StreamElement>> result = (List) compiled.run();
    Map<String, String> resultMap =
        result
            .stream()
            .filter(p -> p.getSecond() != null)
            .collect(Collectors.toMap(p -> p.getFirst().getKey(), p -> p.getSecond().getKey()));
    assertEquals(2, resultMap.size());
    assertEquals("key2", resultMap.get("key2"));
    assertEquals("key3", resultMap.get("key3"));
  }

  @Test
  public void testGroupReduceConsumed() throws Exception {
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates()"
                + ".groupReduce({ it.key }, { w, el -> [[w.toString(), el.size()]] })"
                + ".filter({ true })"
                + ".collect()");

    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid1",
            "key1",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key2",
            wildcard.toAttributePrefix() + "2",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid3",
            "key1",
            wildcard.toAttributePrefix() + "3",
            System.currentTimeMillis(),
            new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Pair<Object, List<Object>>> result = (List) compiled.run();
    Map<Object, List<Object>> resultMap =
        result.stream().collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    assertEquals(2, resultMap.get("key1").get(1));
    assertEquals(1, resultMap.get("key2").get(1));
  }

  @Test
  public void testIntegratePerKey() throws Exception {
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates()"
                + ".integratePerKey({ it.key }, { 1 }, { 0 }, { a, b -> a + b })"
                + ".collect()");

    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid1",
            "key1",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key2",
            wildcard.toAttributePrefix() + "2",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid3",
            "key1",
            wildcard.toAttributePrefix() + "3",
            System.currentTimeMillis(),
            new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Pair<Object, Object>> result = (List) compiled.run();
    Map<Object, List<Object>> resultMap =
        result
            .stream()
            .collect(
                Collectors.groupingBy(
                    Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList())));
    assertEquals(Arrays.asList(1, 2), resultMap.get("key1"));
    assertEquals(Collections.singletonList(1), resultMap.get("key2"));
  }

  @Test
  public void testReduceValueStateByKey() throws Exception {
    int prefixLen = wildcard.toAttributePrefix().length();
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates()"
                + ".reduceValueStateByKey("
                + "{ it.key }, { Integer.valueOf(it.attribute.substring("
                + prefixLen
                + ")) }"
                + ", { 0 }, { s, v -> v - s }, { s, v -> v })"
                + ".collect()");

    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid1",
            "key1",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key2",
            wildcard.toAttributePrefix() + "2",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid3",
            "key1",
            wildcard.toAttributePrefix() + "3",
            System.currentTimeMillis(),
            new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Pair<Object, Object>> result = (List) compiled.run();
    Map<Object, List<Object>> resultMap =
        result
            .stream()
            .collect(
                Collectors.groupingBy(
                    Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList())));
    assertEquals(Arrays.asList(1, 2), resultMap.get("key1"));
    assertEquals(Collections.singletonList(2), resultMap.get("key2"));
  }

  @Test
  public void testReduceValueWithIntegratePerKey() throws Exception {
    int prefixLen = wildcard.toAttributePrefix().length();
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates()"
                // take only changes in value per key
                + ".reduceValueStateByKey("
                + " { it.key },"
                + "{ Integer.valueOf(it.attribute["
                + prefixLen
                + "]) }, "
                + "{ 0 }, {s, v -> v - s}, {s, v -> v})"
                // and running aggregate
                + ".integratePerKey({ \"\" }, { it.second }, { 0 }, {a, b -> a + b})"
                + ".map({ it.second })"
                + ".withTimestamp()"
                + ".collect()");

    // the InMemStorage is not append storage, so we need
    // to append additional suffix to the attribute name with ID of write
    // operation (1..5). That is ignored during value extraction in
    // reduceValueStateByKey
    long now = System.currentTimeMillis();
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid1",
            "key1",
            wildcard.toAttributePrefix() + "11",
            now,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key1",
            wildcard.toAttributePrefix() + "02",
            now + 1,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid3",
            "key2",
            wildcard.toAttributePrefix() + "13",
            now + 2,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid4",
            "key1",
            wildcard.toAttributePrefix() + "14",
            now + 3,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid5",
            "key1",
            wildcard.toAttributePrefix() + "15",
            now + 4,
            new byte[] {}));

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Integer> result =
        (List)
            ((List) compiled.run())
                .stream()
                .sorted(Comparator.comparing(p -> ((Pair<Object, Comparable>) p).getSecond()))
                .map(e -> ((Pair<Object, Object>) e).getFirst())
                .collect(Collectors.toList());
    assertEquals(Arrays.asList(1, 0, 1, 2, 2), result);
  }

  @Test
  public void testReduceValueStateByKeyWithSameStamp() throws Exception {

    int prefixLen = wildcard.toAttributePrefix().length();
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates()"
                + ".flatMap({ [1, 2].collect({ i -> "
                + "new Tuple(it.key, i + Integer.valueOf(it.attribute["
                + prefixLen
                + "])) }) })"
                + ".reduceValueStateByKey("
                + " { it[0] }, { it[1] }, "
                + "{ 0 }, {s, v -> v - s}, {s, v -> v})"
                + ".map({ it.second })"
                + ".withTimestamp()"
                + ".collect()");

    // the InMemStorage is not append storage, so we need
    // to append additional suffix to the attribute name with ID of write
    // operation (1..5). That is ignored during value extraction in
    // reduceValueStateByKey
    long now = System.currentTimeMillis();
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid1",
            "key1",
            wildcard.toAttributePrefix() + "11",
            now,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key1",
            wildcard.toAttributePrefix() + "02",
            now + 1,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid3",
            "key2",
            wildcard.toAttributePrefix() + "13",
            now + 2,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid4",
            "key1",
            wildcard.toAttributePrefix() + "14",
            now + 3,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid5",
            "key1",
            wildcard.toAttributePrefix() + "15",
            now + 4,
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Pair<Integer, Long>> result = (List) compiled.run();
    assertUnorderedEquals(
        Arrays.asList(
            Pair.of(2, now), Pair.of(1, now),
            Pair.of(-2, now + 1), Pair.of(1, now + 1),
            Pair.of(2, now + 2), Pair.of(1, now + 2),
            Pair.of(0, now + 3), Pair.of(1, now + 3),
            Pair.of(-1, now + 4), Pair.of(1, now + 4)),
        result);
  }

  @Test
  public void testReduceValueStateWithLatenessAndSlidingWindow() throws Exception {
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates()"
                + ".map({ new Tuple(it.key, it.stamp) })"
                + ".windowAll().withAllowedLateness(100)"
                + ".reduceValueStateByKey("
                + " { it[0] }, { it[1] }, "
                + "{ Long.MIN_VALUE }, {s, v -> 1 }, "
                + " {s, v -> v})"
                + ".timeSlidingWindow(10000, 1000)"
                + ".countByKey({ it.first })"
                + ".map({ it.second })"
                + ".collect()");

    // make sure that the generated elements fit into the same 1s windows
    long now = System.currentTimeMillis() / 1000 * 1000 + 500;
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid1",
            "key1",
            wildcard.toAttributePrefix() + "11",
            now,
            new byte[] {}));

    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key2",
            wildcard.toAttributePrefix() + "11",
            now + 1,
            new byte[] {}));

    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid3",
            "key1",
            wildcard.toAttributePrefix() + "12",
            now + 2,
            new byte[] {}));

    @SuppressWarnings("unchecked")
    List<Integer> result =
        ((List<Long>) compiled.run())
            .stream()
            .sorted()
            .map(e -> (int) (long) e)
            .collect(Collectors.toList());
    assertEquals(Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2), result);
  }

  @Test
  public void testUnionOnDifferentWindows() throws Exception {
    Script compiled =
        compile(
            "env.batch.data.batchUpdates().count().union(env.batch.wildcard"
                + ".batchUpdates().timeWindow(5000).count()).collect()");
    write(
        StreamElement.upsert(
            batch,
            data,
            "uuid1",
            "key1",
            data.getName(),
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid2",
            "key2",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Long> result = (List) compiled.run();
    assertEquals(2, result.size());
  }

  @Test
  public void testUnionOnDifferentWindowsDifferentTrigger() throws Exception {
    Script compiled =
        compile(
            "env.batch.data.batchUpdates().count().union(env.batch.wildcard"
                + ".batchUpdates().count()).collect()");
    write(
        StreamElement.upsert(
            batch, data, "uuid", "key", data.getName(), System.currentTimeMillis(), new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis(),
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Long> result = (List) compiled.run();
    assertEquals(2, result.size());
  }

  @Test
  public void testIntegratePerKeyAfterWindowing() throws Exception {
    Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates().timeWindow(1000).count()"
                + ".windowAll().integratePerKey({ \"\" }, { it }, { 0 }, {a, b -> a + b})"
                + ".collect()");
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key",
            wildcard.toAttributePrefix() + "0",
            System.currentTimeMillis(),
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key",
            wildcard.toAttributePrefix() + "1",
            System.currentTimeMillis() + 2000,
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Pair<String, Long>> result = (List) compiled.run();
    assertEquals(2, result.size());
    assertEquals(Arrays.asList(Pair.of("", 1L), Pair.of("", 2L)), result);
  }

  @Test
  public void testSumDistinctSlidingWindow() throws Exception {
    long now = 0L;
    final Script compiled =
        compile(
            "env.batch.wildcard.batchUpdates()"
                + ".timeSlidingWindow(1000, 500)"
                + ".map({ it.key })"
                + ".distinct().count().collect()");
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key",
            wildcard.toAttributePrefix() + "0",
            now + 1,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key2",
            wildcard.toAttributePrefix() + "0",
            now + 50,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key",
            wildcard.toAttributePrefix() + "1",
            now + 700,
            new byte[] {}));
    write(
        StreamElement.upsert(
            batch,
            wildcard,
            "uuid",
            "key3",
            wildcard.toAttributePrefix() + "1",
            now + 800,
            new byte[] {}));
    @SuppressWarnings({"unchecked", "rawtypes"})
    List<Long> result = (List) compiled.run();
    assertEquals(3, result.size());
    assertUnorderedEquals(Arrays.asList(2L, 3L, 2L), result);
  }

  protected abstract void write(StreamElement element);

  protected Repository getRepo() {
    return repo;
  }

  private <T> void assertUnorderedEquals(List<T> expected, List<T> actual) {
    assertEquals(getCounts(expected), getCounts(actual));
  }

  private <T> Map<T, Integer> getCounts(List<T> expected) {
    return expected
        .stream()
        .collect(
            Collectors.groupingBy(
                Function.identity(),
                Collectors.mapping(a -> 1, Collectors.reducing(0, Integer::sum))));
  }
}
