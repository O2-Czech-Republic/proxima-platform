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
package cz.o2.proxima.replication;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.TransformationDescriptor;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.transform.ElementWiseProxyTransform;
import cz.o2.proxima.util.TransformationRunner;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

/**
 * More complex test case with single shared topic on replication input and output and proxy
 * attribute.
 */
public class SingleTopicMultipleReplicationsTest {

  public static class WildcardToRawTransform implements ElementWiseProxyTransform {

    @Override
    public String fromProxy(String proxy) {
      return "_raw." + String.valueOf(Integer.valueOf(proxy.substring(9)) + 1);
    }

    @Override
    public String toProxy(String raw) {
      return "wildcard." + String.valueOf(Integer.valueOf(raw.substring(5)) - 1);
    }
  }

  final Repository repo =
      Repository.of(ConfigFactory.parseResources("test-replication-single-topic.conf"));
  final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  final EntityDescriptor entity =
      repo.findEntity("entity")
          .orElseThrow(() -> new IllegalStateException("Missing entity entity"));
  final AttributeDescriptor<byte[]> scalar = findAttribute("scalar");
  final AttributeDescriptor<byte[]> wildcard = findAttribute("wildcard.*");
  final AttributeDescriptor<byte[]> raw = findAttribute("_raw.*");
  final AttributeDescriptor<byte[]> wildcardInput =
      findAttribute("_wildcardReplication_read$wildcard.*");
  final AttributeDescriptor<byte[]> rawReplicated =
      findAttribute("_wildcardReplication_replicated$_raw.*");
  final AttributeDescriptor<byte[]> rawWrite = findAttribute("_wildcardReplication_write$_raw.*");

  long now;

  @Before
  public void setUp() {
    TransformationRunner.runTransformations(repo, direct);
    now = System.currentTimeMillis();
  }

  @Test
  public void testTransformationWildcardInput() {
    TransformationDescriptor wildcardInputTransform =
        repo.getTransformations().get("_wildcardReplication_read");
    assertNotNull(wildcardInputTransform);
    assertEquals(1, wildcardInputTransform.getAttributes().size());
    assertEquals(wildcardInput, wildcardInputTransform.getAttributes().get(0));
    List<StreamElement> transformed = new ArrayList<>();
    int transformedCount =
        wildcardInputTransform
            .getTransformation()
            .asElementWiseTransform()
            .apply(
                StreamElement.upsert(entity, wildcard, uuid(), "key", "wildcard.1", now, value()),
                transformed::add);

    assertEquals(1, transformedCount);
    assertEquals(1, transformed.size());
    assertEquals(
        "_wildcardReplication_replicated$_raw.*",
        transformed.get(0).getAttributeDescriptor().getName());
    assertEquals("_raw.2", transformed.get(0).getAttribute());
  }

  @Test
  public void testTransformationWildcardWriteRead() {
    TransformationDescriptor wildcardReplicatedTransform =
        repo.getTransformations().get("_wildcardReplication_replicated");
    assertNotNull(wildcardReplicatedTransform);
    assertEquals(1, wildcardReplicatedTransform.getAttributes().size());
    assertEquals(rawWrite, wildcardReplicatedTransform.getAttributes().get(0));
    List<StreamElement> transformed = new ArrayList<>();
    int transformedCount =
        wildcardReplicatedTransform
            .getTransformation()
            .asElementWiseTransform()
            .apply(
                StreamElement.upsert(entity, raw, uuid(), "key", "_raw.2", now, value()),
                transformed::add);

    assertEquals(1, transformedCount);
    assertEquals(1, transformed.size());
    assertEquals(
        "_wildcardReplication_replicated$_raw.*",
        transformed.get(0).getAttributeDescriptor().getName());
    assertEquals("_raw.2", transformed.get(0).getAttribute());
  }

  @Test(timeout = 5000)
  public void testProxyReadWildcardReplicated() throws InterruptedException {
    Optional<AttributeFamilyDescriptor> wildcardPrimary =
        repo.getFamiliesForAttribute(wildcard)
            .stream()
            .filter(af -> af.getType() == StorageType.PRIMARY)
            .findFirst();
    assertTrue(wildcardPrimary.isPresent());
    AttributeFamilyDescriptor family = wildcardPrimary.get();
    assertEquals(
        "proxy::proxy::replication_wildcard-replication_replicated::"
            + "replication_wildcard-replication_write::proxy::"
            + "replication_wildcard-replication_replicated::"
            + "replication_wildcard-replication_write",
        family.getName());
    assertTrue(family.getAccess().canReadCommitLog());
    CommitLogReader reader =
        direct
            .resolveRequired(family)
            .getCommitLogReader()
            .orElseThrow(() -> new IllegalStateException("Missing commit-log in " + family));
    CountDownLatch latch = new CountDownLatch(1);
    reader
        .observe(
            "dummy",
            new LogObserver() {
              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                assertEquals(wildcard, ingest.getAttributeDescriptor());
                assertEquals("wildcard.1", ingest.getAttribute());
                latch.countDown();
                context.confirm();
                return false;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            })
        .waitUntilReady();
    OnlineAttributeWriter writer =
        direct
            .getWriter(wildcardInput)
            .orElseThrow(() -> new IllegalStateException("Missing writer for " + wildcardInput));
    writer.write(
        StreamElement.upsert(
            entity, wildcard, uuid(), "key", wildcard.toAttributePrefix() + "1", now, value()),
        (succ, exc) -> {});
    assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @Test(timeout = 5000)
  public void testProxyObserveBulkPartitionsWildcardReplicated() throws InterruptedException {

    Optional<AttributeFamilyDescriptor> wildcardPrimary =
        repo.getFamiliesForAttribute(wildcard)
            .stream()
            .filter(af -> af.getType() == StorageType.PRIMARY)
            .findFirst();
    assertTrue(wildcardPrimary.isPresent());
    AttributeFamilyDescriptor family = wildcardPrimary.get();
    assertTrue(family.getAccess().canReadCommitLog());
    CommitLogReader reader =
        direct
            .resolveRequired(family)
            .getCommitLogReader()
            .orElseThrow(() -> new IllegalStateException("Missing commit-log in " + family));
    CountDownLatch latch = new CountDownLatch(1);
    reader
        .observeBulkPartitions(
            reader.getPartitions(),
            Position.CURRENT,
            new LogObserver() {
              @Override
              public boolean onNext(StreamElement ingest, OnNextContext context) {
                assertEquals(wildcard, ingest.getAttributeDescriptor());
                assertEquals("wildcard.1", ingest.getAttribute());
                latch.countDown();
                return false;
              }

              @Override
              public boolean onError(Throwable error) {
                throw new RuntimeException(error);
              }
            })
        .waitUntilReady();
    OnlineAttributeWriter writer =
        direct
            .getWriter(wildcardInput)
            .orElseThrow(() -> new IllegalStateException("Missing writer for " + wildcardInput));
    writer.write(
        StreamElement.upsert(
            entity, wildcard, uuid(), "key", wildcard.toAttributePrefix() + "1", now, value()),
        (succ, exc) -> {});
    assertTrue(latch.await(1, TimeUnit.SECONDS));
  }

  @SuppressWarnings("unchecked")
  private AttributeDescriptor<byte[]> findAttribute(String name) {
    return (AttributeDescriptor)
        entity
            .findAttribute(name, true)
            .orElseThrow(() -> new IllegalStateException("Missing attribute " + name));
  }

  private String uuid() {
    return UUID.randomUUID().toString();
  }

  private byte[] value() {
    return new byte[] {1, 2, 3};
  }
}
