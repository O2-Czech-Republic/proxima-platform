/**
 * Copyright 2017 O2 Czech Republic, a.s.
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

package cz.o2.proxima.storage.kafka;

import cz.o2.proxima.storage.kafka.OffsetCommitter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test {@code OffsetCommitter}.
 */
public class OffsetCommitterTest {

  OffsetCommitter<String> committer;

  @Before
  public void setUp() {
    committer = new OffsetCommitter<>();
  }

  /**
   * Test simple commit.
   */
  @Test
  public void testSimpleCommit() {
    String id = "dummy-0";
    AtomicBoolean committed = new AtomicBoolean();
    long offset = 1;
    committer.register(id, offset, 5, () -> committed.set(true));
    assertFalse(committed.get());
    for (int i = 0; i < 5; i++) {
      committer.confirm(id, offset);
    }
    assertTrue(committed.get());
  }

  /**
   * Test commit with two offsets.
   */
  @Test
  public void testTwoOffsetCommit() {
    String id = "dummy-0";
    AtomicLong committed = new AtomicLong();
    long offset = 1;
    committer.register(id, offset, 5, () -> committed.set(offset));
    committer.register(id, offset + 1, 6, () -> committed.set(offset + 1));
    for (int i = 0; i < 6; i++) {
      committer.confirm(id, offset + 1);
    }
    assertEquals(0, committed.get());
    for (int i = 0; i < 4; i++) {
      committer.confirm(id, offset);
    }
    assertEquals(0, committed.get());
    committer.confirm(id, offset);
    assertEquals(offset + 1, committed.get());
  }

  @Test
  public void testFourOffsetCommit() {
    String id = "dummy-0";
    AtomicLong committed = new AtomicLong();
    long offset = 1;
    committer.register(id, offset, 5, () -> committed.set(offset));
    committer.register(id, offset + 1, 6, () -> committed.set(offset + 1));
    committer.register(id, offset + 2, 4, () -> committed.set(offset + 2));
    committer.register(id, offset + 3, 3, () -> committed.set(offset + 3));
    for (int i = 0; i < 4; i++) {
      committer.confirm(id, offset + 2);
    }
    assertEquals(0, committed.get());
    for (int i = 0; i < 6; i++) {
      committer.confirm(id, offset + 1);
    }
    assertEquals(0, committed.get());
    for (int i = 0; i < 5; i++) {
      committer.confirm(id, offset);
    }
    assertEquals(offset + 2, committed.get());
  }

  @Test
  public void testCommitWithZeroActionsOnly() {
    String id = "dummy-0";
    AtomicLong committed = new AtomicLong();
    long offset = 1;
    committer.register(id, offset, 0, () -> committed.set(offset));
    assertEquals(offset, committed.get());
  }


  @Test
  public void testCommitWithZeroActions() {
    String id = "dummy-0";
    AtomicLong committed = new AtomicLong();
    long offset = 1;
    committer.register(id, offset, 5, () -> committed.set(offset));
    committer.register(id, offset + 1, 0, () -> committed.set(offset + 1));
    assertEquals(0, committed.get());
    for (int i = 0; i < 4; i++) {
      committer.confirm(id, offset);
    }
    assertEquals(0, committed.get());
    committer.confirm(id, offset);
    assertEquals(offset + 1, committed.get());
  }


}
