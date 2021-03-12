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
package cz.o2.proxima.beam.io.pubsub;

import static org.junit.Assert.assertEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.DataAccessor;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.util.TestUtils;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Before;
import org.junit.Test;

/** Test suite for {@link PubSubDataAccessor}. */
public class PubSubDataAccessorTest {

  final Repository repo = Repository.of(ConfigFactory.load("test-reference.conf"));
  final EntityDescriptor gateway =
      repo.findEntity("gateway")
          .orElseThrow(() -> new IllegalArgumentException("Missing entity gateway"));
  BeamDataOperator operator;
  DataAccessor accessor;

  @Before
  public void setUp() {
    operator = repo.getOrCreateOperator(BeamDataOperator.class);
    accessor =
        new PubSubDataAccessorFactory()
            .createAccessor(
                operator,
                TestUtils.createTestFamily(gateway, uri(), gateway.getAllAttributes(), cfg()));
  }

  @Test
  public void testCoderAndType() {
    Pipeline pipeline = Pipeline.create();
    PCollection<StreamElement> input =
        accessor.createStream("name", pipeline, Position.NEWEST, false, true, 1);
    assertEquals(TypeDescriptor.of(StreamElement.class), input.getTypeDescriptor());
    assertEquals(StreamElementCoder.of(repo), input.getCoder());
  }

  @Test
  public void testCoderAndTypeProcessingTime() {
    Pipeline pipeline = Pipeline.create();
    PCollection<StreamElement> input =
        accessor.createStream("name", pipeline, Position.NEWEST, false, false, 1);
    assertEquals(TypeDescriptor.of(StreamElement.class), input.getTypeDescriptor());
    assertEquals(StreamElementCoder.of(repo), input.getCoder());
  }

  private URI uri() {
    try {
      return new URI("gps://my-project/my_topic");
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  private Map<String, Object> cfg() {
    return Collections.emptyMap();
  }
}
