/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.gcloud.storage;

import static org.junit.Assert.*;
import org.junit.Test;

/**
 * Test suite for {@link GCloudLogObservableTest}.
 */
public class GCloudLogObservableTest {

  /**
   * Test filtering of partitions.
   */
  @Test
  public void testPartitionsRange() {

    assertTrue(GCloudLogObservable.isInRange(
        "prefix-1234567890000_9876543210000.blob.whatever",
        1234567890000L, 12345678901000L));
    assertTrue(GCloudLogObservable.isInRange(
        "prefix-1234567890000_9876543210000.blob",
        1234567891000L, 12345678902000L));
    assertTrue(GCloudLogObservable.isInRange(
        "/my/dummy/path/prefix-1234567890000_9876543210000.blob",
        1234567891000L, 12345678902000L));
    assertTrue(GCloudLogObservable.isInRange(
        "/my/dummy/path/prefix-1234567890000_9876543210000_suffix.blob",
        1234567891000L, 12345678902000L));
    assertTrue(GCloudLogObservable.isInRange(
        "prefix-1234567890000_9876543210000.blob.whatever",
        1234567891000L, 12345678902000L));
    assertTrue(GCloudLogObservable.isInRange(
        "prefix-1234567890000_9876543210000.blob.whatever",
        1234567880000L, 12345678902000L));
    assertTrue(GCloudLogObservable.isInRange(
        "prefix-1234567890000_9876543210000.blob.whatever",
        9876543200000L, 9999999999999L));

    assertFalse(GCloudLogObservable.isInRange(
        "prefix-1234567890000_9876543210000.blob.whatever",
        1234567880000L, 1234567881000L));
    assertFalse(GCloudLogObservable.isInRange(
        "prefix-1234567890000_9876543210000.blob.whatever",
        9999999999000L, 9999999999999L));
  }

}
