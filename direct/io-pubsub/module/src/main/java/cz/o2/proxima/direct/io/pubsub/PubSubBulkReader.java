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
package cz.o2.proxima.direct.io.pubsub;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.google.pubsub.v1.PubsubMessage;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import cz.o2.proxima.io.pubsub.proto.PubSub.Bulk;
import cz.o2.proxima.io.pubsub.proto.PubSub.BulkWrapper;
import cz.o2.proxima.io.pubsub.proto.PubSub.BulkWrapper.Compression;
import cz.o2.proxima.io.pubsub.util.PubSubUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.InflaterInputStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PubSubBulkReader extends AbstractPubSubReader implements CommitLogReader {

  public PubSubBulkReader(PubSubAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getUri(), accessor, context);
    Preconditions.checkArgument(accessor.isBulk());
  }

  @Override
  protected List<StreamElement> parseElements(PubsubMessage m) {
    ByteString data = m.getData();
    try {
      Bulk bulk = Bulk.parseFrom(deserialize(data));
      int bulkSize = bulk.getKvCount();
      if (bulk.getUuidCount() != bulkSize) {
        log.warn("Invalid bulk {}", TextFormat.shortDebugString(bulk));
        return Collections.emptyList();
      }
      List<StreamElement> ret = new ArrayList<>(bulkSize);
      for (int i = 0; i < bulkSize; i++) {
        PubSubUtils.toStreamElement(getEntityDescriptor(), bulk.getUuid(i), bulk.getKv(i))
            .ifPresent(ret::add);
      }
      return ret;
    } catch (IOException e) {
      log.warn("Failed to parse message from {}", m, e);
      return Collections.emptyList();
    }
  }

  private byte[] deserialize(ByteString data) throws IOException {
    BulkWrapper wrapper = BulkWrapper.parseFrom(data);
    if (wrapper.getCompression().equals(Compression.DEFLATE)) {
      return inflate(wrapper.getValue().toByteArray());
    }
    return wrapper.getValue().toByteArray();
  }

  static byte[] inflate(byte[] data) throws IOException {
    try (InflaterInputStream in = new InflaterInputStream(new ByteArrayInputStream(data))) {
      return in.readAllBytes();
    }
  }

  public Factory<?> asFactory() {
    final PubSubAccessor accessor = this.accessor;
    final Context context = this.context;
    return repo -> new PubSubBulkReader(accessor, context);
  }
}
