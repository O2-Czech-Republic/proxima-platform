/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.kafka;

import cz.o2.proxima.core.util.TestUtils;
import org.junit.Test;

public class TopicOffsetTest {

  @Test
  public void testHashCodeEquals() {
    TopicOffset off1 = new TopicOffset(new PartitionWithTopic("topic", 0), 1, 2);
    TopicOffset off2 = new TopicOffset(new PartitionWithTopic("topic", 0), 1, 2);
    TestUtils.assertHashCodeAndEquals(off1, off2);
  }
}
