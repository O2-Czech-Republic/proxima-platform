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
package cz.o2.proxima.beam.direct.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.direct.io.DirectUnboundedSource.Checkpoint;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.util.Optionals;
import cz.o2.proxima.util.SerializableUtils;
import cz.o2.proxima.util.TestUtils;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;

public class DirectUnboundedSourceTest {

  Repository repo;
  EntityDescriptor gateway;
  AttributeDescriptor<Object> armed;
  DirectDataOperator direct;

  @Before
  public void setUp() {
    repo = Repository.ofTest(ConfigFactory.load("test-reference.conf").resolve());
    gateway = repo.getEntity("gateway");
    armed = gateway.getAttribute("armed");
    direct = repo.getOrCreateOperator(DirectDataOperator.class);
  }

  @Test
  public void testCheckpointWithNoAdvance() throws Exception {
    OnlineAttributeWriter writer = Optionals.get(direct.getWriter(armed));
    CommitLogReader commitLogReader = Optionals.get(direct.getCommitLogReader(armed));
    DirectUnboundedSource source =
        DirectUnboundedSource.of(
            repo.asFactory(), "name", commitLogReader, Position.OLDEST, true, Long.MAX_VALUE);
    long now = System.currentTimeMillis();
    writer.write(
        StreamElement.upsert(
            gateway,
            armed,
            UUID.randomUUID().toString(),
            "key",
            armed.getName(),
            now,
            new byte[] {}),
        (succ, exc) -> {});
    PipelineOptions opts = PipelineOptionsFactory.create();
    UnboundedSource<StreamElement, Checkpoint> split = source.split(1, opts).get(0);
    UnboundedReader<StreamElement> reader = split.createReader(opts, null);
    boolean start = reader.start();
    while (!start && !reader.advance()) {
      TimeUnit.MILLISECONDS.sleep(10);
    }
    Checkpoint mark = (Checkpoint) reader.getCheckpointMark();
    assertNotNull(mark);
    reader = split.createReader(opts, mark);
    TestUtils.assertSerializable(source);
    TestUtils.assertHashCodeAndEquals(source, SerializableUtils.clone(source));
    assertEquals(mark, reader.getCheckpointMark());
    TestUtils.assertSerializable(mark);
    TestUtils.assertHashCodeAndEquals(mark, SerializableUtils.clone(mark));
  }
}
