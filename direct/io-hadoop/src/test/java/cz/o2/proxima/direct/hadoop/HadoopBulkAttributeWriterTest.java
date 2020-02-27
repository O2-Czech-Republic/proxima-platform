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
package cz.o2.proxima.direct.hadoop;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.util.TestUtils;
import java.net.URI;
import java.util.Collections;
import org.junit.Test;

/** Simple tests for {@code HadoopBulkAttributeWriter}. */
public class HadoopBulkAttributeWriterTest {

  private final Repository repo =
      Repository.of(() -> ConfigFactory.load("test-reference.conf").resolve());
  private final DirectDataOperator direct = repo.getOrCreateOperator(DirectDataOperator.class);
  private final EntityDescriptor entity = repo.getEntity("gateway");
  private final HadoopDataAccessor accessor =
      new HadoopDataAccessor(entity, URI.create("hadoop-file://dummy/dir"), Collections.emptyMap());

  @Test
  public void testSerializable() throws Exception {
    HadoopBulkAttributeWriter writer = new HadoopBulkAttributeWriter(accessor, direct.getContext());
    TestUtils.assertSerializable(writer);
  }
}
