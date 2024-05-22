/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.pubsub;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.Pair;
import cz.o2.proxima.direct.core.CommitCallback;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.OnlineAttributeWriter;
import cz.o2.proxima.io.pubsub.proto.PubSub.Bulk;
import cz.o2.proxima.io.pubsub.proto.PubSub.BulkWrapper;
import cz.o2.proxima.io.pubsub.proto.PubSub.BulkWrapper.Compression;
import cz.o2.proxima.io.pubsub.util.PubSubUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.DeflaterOutputStream;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class PubSubBulkWriter extends AbstractPubSubWriter implements OnlineAttributeWriter {

  private final Bulk.Builder bulk = Bulk.newBuilder();

  // guarded by bulk
  private final List<CommitCallback> uncommitted = new ArrayList<>();

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private volatile @Nullable ScheduledFuture<?> flushFuture = null;

  PubSubBulkWriter(PubSubAccessor accessor, Context context) {
    super(accessor, context);
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {
    final @Nullable Pair<Bulk, List<CommitCallback>> toWrite;
    synchronized (bulk) {
      bulk.addKv(PubSubUtils.toKeyValue(data)).addUuid(data.getUuid());
      uncommitted.add(statusCallback);
      toWrite = flushOrSetTimer();
    }
    if (toWrite != null) {
      processFlushedData(toWrite);
    }
  }

  private void processFlushedData(Pair<Bulk, List<CommitCallback>> toWrite) {
    BulkWrapper wrapper =
        BulkWrapper.newBuilder()
            .setCompression(accessor.getBulk().isDeflate() ? Compression.DEFLATE : Compression.NONE)
            .setValue(asByteString(toWrite.getFirst()))
            .build();
    writeMessage(
        UUID.randomUUID().toString(),
        System.currentTimeMillis(),
        wrapper,
        (succ, exc) -> toWrite.getSecond().forEach(c -> c.commit(succ, exc)));
  }

  private ByteString asByteString(Message msg) {
    if (accessor.getBulk().isDeflate()) {
      try {
        return ByteString.copyFrom(deflate(msg.toByteArray()));
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
    return msg.toByteString();
  }

  static byte[] deflate(byte[] data) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (DeflaterOutputStream deflate = new DeflaterOutputStream(out)) {
      deflate.write(data);
    }
    return out.toByteArray();
  }

  private @Nullable Pair<Bulk, List<CommitCallback>> flushOrSetTimer() {
    return flushOrSetTimer(false);
  }

  private void processForcedFlush() {
    Pair<Bulk, List<CommitCallback>> toFlush = flushOrSetTimer(true);
    if (toFlush != null) {
      processFlushedData(toFlush);
    }
  }

  private @Nullable Pair<Bulk, List<CommitCallback>> flushOrSetTimer(boolean force) {
    if (force || bulk.getKvCount() >= accessor.getBulk().getBulkSize()) {
      return flushBuilder();
    }
    if (flushFuture == null) {
      log.debug("Setting up flush future with timeout {}", accessor.getBulk().getFlushMs());
      flushFuture =
          scheduler.schedule(
              this::processForcedFlush, accessor.getBulk().getFlushMs(), TimeUnit.MILLISECONDS);
    }
    return null;
  }

  private Pair<Bulk, List<CommitCallback>> flushBuilder() {
    final Bulk message = bulk.build();
    final List<CommitCallback> callbacks = new ArrayList<>(uncommitted);
    bulk.clear();
    uncommitted.clear();
    if (flushFuture != null) {
      flushFuture.cancel(false);
      flushFuture = null;
    }
    return Pair.of(message, callbacks);
  }

  @Override
  public Factory<PubSubBulkWriter> asFactory() {
    final PubSubAccessor _accessor = accessor;
    final Context _context = context;
    return _ign -> new PubSubBulkWriter(_accessor, _context);
  }
}
