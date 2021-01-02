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
package cz.o2.proxima.direct.time;

import static cz.o2.proxima.direct.time.BoundedOutOfOrdernessWatermarkEstimator.DEFAULT_MAX_OUT_OF_ORDERNESS_MS;
import static cz.o2.proxima.direct.time.BoundedOutOfOrdernessWatermarkEstimator.MAX_OUT_OF_ORDERNESS_MS;
import static cz.o2.proxima.direct.time.WatermarkConfiguration.prefixedKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.time.WatermarkEstimatorFactory;
import cz.o2.proxima.time.WatermarkIdlePolicy;
import cz.o2.proxima.time.WatermarkIdlePolicyFactory;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class BoundedOutOfOrdernessWatermarkEstimatorTest {
  private static final long OUT_OF_ORDERNESS = 1000L;
  private static final long MIN_WATERMARK = 0L;

  private ConfigRepository repo;
  private long now;

  @Before
  public void setup() {
    this.repo =
        ConfigRepository.Builder.of(
                ConfigFactory.load()
                    .withFallback(ConfigFactory.load("test-reference.conf"))
                    .resolve())
            .build();

    this.now = Instant.now().toEpochMilli();
  }

  @Test
  public void testInitialize() {
    BoundedOutOfOrdernessWatermarkEstimator estimator = createEstimator();
    assertEquals(MIN_WATERMARK, estimator.getMinWatermark());
    assertEquals(OUT_OF_ORDERNESS, estimator.getMaxOutOfOrderness());
  }

  @Test
  public void testGetWatermarkWhenNoElement() {
    assertEquals(MIN_WATERMARK, createEstimator().getWatermark());
  }

  @Test
  public void testGetWatermarkWhenMinWatermarkSet() {
    BoundedOutOfOrdernessWatermarkEstimator estimator = createEstimator();
    estimator.setMinWatermark(0L);

    assertEquals(0L, estimator.getWatermark());
  }

  @Test
  public void testGetWatermarkWhenInOrderElements() {
    BoundedOutOfOrdernessWatermarkEstimator estimator = createEstimator();

    estimator.update(element(now));
    estimator.update(element(now + 100));
    estimator.update(element(now + 1000));

    assertEquals(now, estimator.getWatermark());

    estimator.update(element(now + 2000));

    assertEquals(now + 1000, estimator.getWatermark());
  }

  @Test
  public void testGetWatermarkWhenOutOfOrderElements() {
    BoundedOutOfOrdernessWatermarkEstimator estimator = createEstimator();
    estimator.update(element(now));
    estimator.update(element(now + 100));
    estimator.update(element(now + 1000));
    estimator.update(element(now + 200));
    estimator.update(element(now + 600));

    assertEquals(now, estimator.getWatermark());

    estimator.update(element(now + 2000));
    estimator.update(element(now + 1600));

    assertEquals(now + 1000, estimator.getWatermark());
  }

  @Test
  public void testGetWatermarkWhenElementWithTimestampLowerThanMinWatermark() {
    BoundedOutOfOrdernessWatermarkEstimator estimator = createEstimator();
    estimator.update(element(MIN_WATERMARK - 10_000));
    assertEquals(MIN_WATERMARK, estimator.getWatermark());
  }

  @Test
  public void testGetWatermarkMonotonicity() {
    Random random = new Random(now);
    BoundedOutOfOrdernessWatermarkEstimator estimator = createEstimator();
    for (int i = 0; i < 100; i++) {
      long previousWatermark = estimator.getWatermark();
      estimator.update(element(random.nextLong()));
      assertTrue(previousWatermark <= estimator.getWatermark());
    }
  }

  @Test
  public void testGetMaxOutOfOrdernessWhenNotSet() {
    final BoundedOutOfOrdernessWatermarkEstimator estimator =
        BoundedOutOfOrdernessWatermarkEstimator.newBuilder().build();
    assertEquals(DEFAULT_MAX_OUT_OF_ORDERNESS_MS, estimator.getMaxOutOfOrderness());
  }

  @Test
  public void testIdlePolicy() {
    WatermarkIdlePolicy idlePolicy = mock(WatermarkIdlePolicy.class);
    BoundedOutOfOrdernessWatermarkEstimator estimator =
        BoundedOutOfOrdernessWatermarkEstimator.newBuilder()
            .withWatermarkIdlePolicy(idlePolicy)
            .build();
    StreamElement element = element(now);

    estimator.update(element);
    verify(idlePolicy, times(1)).update(element);

    estimator.idle();
    verify(idlePolicy, times(1)).idle(now);
  }

  @Test
  public void testFactory() {
    Map<String, Object> cfg =
        new HashMap<String, Object>() {
          {
            put(prefixedKey(MAX_OUT_OF_ORDERNESS_MS), OUT_OF_ORDERNESS);
          }
        };

    WatermarkIdlePolicyFactory idlePolicyFactory = mock(WatermarkIdlePolicyFactory.class);
    when(idlePolicyFactory.create(cfg)).thenReturn(mock(WatermarkIdlePolicy.class));

    WatermarkEstimatorFactory factory = new BoundedOutOfOrdernessWatermarkEstimator.Factory();
    BoundedOutOfOrdernessWatermarkEstimator watermarkEstimator =
        (BoundedOutOfOrdernessWatermarkEstimator) factory.create(cfg, idlePolicyFactory);

    assertEquals(OUT_OF_ORDERNESS, watermarkEstimator.getMaxOutOfOrderness());
    verify(idlePolicyFactory, times(1)).create(cfg);
  }

  private StreamElement element(long ts) {
    EntityDescriptor dummy = repo.getEntity("dummy");
    AttributeDescriptor<Object> data = dummy.getAttribute("data", true);
    return StreamElement.upsert(dummy, data, UUID.randomUUID().toString(), "key", "attr", ts, null);
  }

  private BoundedOutOfOrdernessWatermarkEstimator createEstimator() {
    return BoundedOutOfOrdernessWatermarkEstimator.newBuilder()
        .withMaxOutOfOrderness(OUT_OF_ORDERNESS)
        .withMinWatermark(MIN_WATERMARK)
        .build();
  }
}
