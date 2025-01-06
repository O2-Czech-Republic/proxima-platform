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
package cz.o2.proxima.direct.core.batch;

import cz.o2.proxima.core.storage.Partition;
import cz.o2.proxima.core.time.Watermarks;
import org.junit.Assert;
import org.junit.Test;

public class BatchLogObserversTest {

  @Test
  public void testDefaultContext() {
    final BatchLogObserver.OnNextContext context =
        BatchLogObservers.defaultContext(Partition.of(3));
    Assert.assertEquals(Partition.of(3), context.getPartition());
    Assert.assertEquals(Watermarks.MIN_WATERMARK, context.getWatermark());
    Assert.assertThrows(UnsupportedOperationException.class, context::getOffset);
  }

  @Test
  public void testWithWatermarkSupplier() {
    final Partition partition = Partition.of(1);
    final Offset offset = Offset.of(partition, 3, false);
    final BatchLogObserver.OnNextContext context =
        BatchLogObservers.withWatermarkSupplier(partition, offset, () -> 1000L);
    Assert.assertEquals(partition, context.getPartition());
    Assert.assertEquals(1000L, context.getWatermark());
    Assert.assertEquals(offset, context.getOffset());
  }

  @Test
  public void testWithWatermark() {
    final Partition partition = Partition.of(1);
    final Offset offset = Offset.of(partition, 3, false);
    final BatchLogObserver.OnNextContext context =
        BatchLogObservers.withWatermark(partition, offset, 2000L);
    Assert.assertEquals(partition, context.getPartition());
    Assert.assertEquals(2000L, context.getWatermark());
    Assert.assertEquals(offset, context.getOffset());
  }
}
