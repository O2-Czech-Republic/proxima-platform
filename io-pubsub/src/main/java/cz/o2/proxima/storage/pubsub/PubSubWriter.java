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
package cz.o2.proxima.storage.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import cz.o2.proxima.repository.Context;
import cz.o2.proxima.storage.AbstractOnlineAttributeWriter;
import cz.o2.proxima.storage.CommitCallback;
import cz.o2.proxima.storage.OnlineAttributeWriter;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.pubsub.proto.PubSub;
import cz.seznam.euphoria.shadow.com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link OnlineAttributeWriter} for Google PubSub.
 */
@Slf4j
class PubSubWriter extends AbstractOnlineAttributeWriter
    implements OnlineAttributeWriter {

  final Publisher publisher;
  final Executor executor;

  PubSubWriter(PubSubAccessor accessor, Context context) {
    super(accessor.getEntityDescriptor(), accessor.getURI());
    try {
      this.publisher = newPublisher(accessor.getProject(), accessor.getTopic());
      this.executor = context.getExecutorService();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @VisibleForTesting
  Publisher newPublisher(String project, String topic) throws IOException {
    return Publisher.newBuilder(TopicName.of(project, topic)).build();
  }

  @Override
  public void write(StreamElement data, CommitCallback statusCallback) {

    log.debug("Writing data {} to {}", data, getURI());
    try {
      ApiFuture<String> future = publisher.publish(PubsubMessage.newBuilder()
          .setMessageId(data.getUuid())
          .setPublishTime(Timestamp.newBuilder()
              .setSeconds(data.getStamp() / 1000)
              .setNanos((int) ((data.getStamp() % 1000)) * 1_000_000))
          .setData(PubSub.KeyValue.newBuilder()
              .setKey(data.getKey())
              .setAttribute(data.getAttribute())
              .setDelete(data.isDelete())
              .setDeleteWildcard(data.isDeleteWildcard())
              .setValue(data.isDelete()
                  ? ByteString.EMPTY
                  : ByteString.copyFrom(data.getValue()))
              .build()
              .toByteString())
          .build());

      ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

        @Override
        public void onFailure(Throwable thrwbl) {
          log.warn("Failed to publish elemet {} to pubsub", data, thrwbl);
          statusCallback.commit(false, thrwbl);
        }

        @Override
        public void onSuccess(String v) {
          log.debug("Committing processing of {} with success", data);
          statusCallback.commit(true, null);
        }

      }, executor);
    } catch (Throwable err) {
      log.warn("Failed to publish {} to pubsub", data, err);
      statusCallback.commit(false, err);
    }
  }

}
