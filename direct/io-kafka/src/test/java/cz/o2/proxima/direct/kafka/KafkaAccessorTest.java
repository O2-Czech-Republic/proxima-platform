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
package cz.o2.proxima.direct.kafka;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.util.ExceptionUtils;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mockito;

public class KafkaAccessorTest {

  // mocks and config structures
  private static AdminClient adminClient;
  private static KafkaFuture kafkaFuture;
  private static Map map;
  private static DescribeConfigsResult cfgResult;
  private static AttributeFamilyDescriptor attrFmlDesc;
  private static AccessType accessType;
  private static List<Config> cfgs;
  private static KafkaAccessor kafkaAccessor;
  private static List<ConfigEntry> cfgEtrs;

  private static void setupMocks() throws ExecutionException, InterruptedException {
    // mocks needed for when/thenReturn
    adminClient = Mockito.mock(AdminClient.class);
    kafkaFuture = mock(KafkaFuture.class);
    cfgResult = mock(DescribeConfigsResult.class);
    map = mock(HashMap.class);
    attrFmlDesc = mock(AttributeFamilyDescriptor.class);
    accessType = mock(AccessType.class);
    cfgs = new ArrayList<>();
    cfgEtrs = new ArrayList<>();

    // return Collection(Config) from describeConfigResult
    when(adminClient.describeConfigs(anyCollectionOf(ConfigResource.class))).thenReturn(cfgResult);
    when(cfgResult.all()).thenReturn(kafkaFuture);
    when(kafkaFuture.get()).thenReturn(map);
    when(map.values()).thenReturn(cfgs);

    // This topic is state-commit-log and has cleanup_policy set
    when(attrFmlDesc.getAccess()).thenReturn(accessType);

    kafkaAccessor =
        new KafkaAccessor(
            EntityDescriptor.newBuilder().setName("entity").build(),
            URI.create("kafka-test://dummy/topic"),
            new HashMap<>()) {
          AdminClient createAdmin() {
            return adminClient;
          }
        };
  }

  @After
  public void tearDown() {}

  @Test
  public void testIsStateCommitLogCleanupCompactAndDeleteMultipleCfgs() {
    ExceptionUtils.unchecked(KafkaAccessorTest::setupMocks);

    when(accessType.isStateCommitLog()).thenReturn(true);
    cfgs.add(new Config(new ArrayList<>()));
    assertFalse(kafkaAccessor.isAcceptable(attrFmlDesc));
    cfgs.clear();

    cfgEtrs.add(
        new ConfigEntry(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_DELETE + "," + TopicConfig.CLEANUP_POLICY_COMPACT));
    cfgEtrs.add(new ConfigEntry(TopicConfig.DELETE_RETENTION_MS_CONFIG, "300000"));
    cfgEtrs.add(new ConfigEntry(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, "300000"));
    cfgs.add(new Config(cfgEtrs));
    assertTrue(kafkaAccessor.isAcceptable(attrFmlDesc));
  }

  @Test
  public void testIsAcceptableStateCommitLog() {
    ExceptionUtils.unchecked(KafkaAccessorTest::setupMocks);

    when(accessType.isStateCommitLog()).thenReturn(true);
    assertTrue(
        kafkaAccessor.verifyCleanupPolicy(
            new ConfigEntry(
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)));
    assertTrue(
        kafkaAccessor.verifyCleanupPolicy(
            new ConfigEntry(
                TopicConfig.CLEANUP_POLICY_CONFIG,
                TopicConfig.CLEANUP_POLICY_DELETE + "," + TopicConfig.CLEANUP_POLICY_COMPACT)));

    assertFalse(
        kafkaAccessor.verifyCleanupPolicy(new ConfigEntry("random_config", "random_value")));
    assertFalse(
        kafkaAccessor.verifyCleanupPolicy(
            new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)));
  }
}
