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
package cz.o2.proxima.storage.kafka;

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.BulkLogObserver;
import cz.o2.proxima.storage.commitlog.LogObserver;
import cz.o2.proxima.storage.commitlog.Offset;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.kafka.common.TopicPartition;

/**
 * Placeholder class for consumers.
 */
class Consumers {

  static final class OnlineConsumer implements ElementConsumer {
    final LogObserver observer;
    final TopicPartitionCommitter committer;
    OnlineConsumer(LogObserver observer, TopicPartitionCommitter committer) {
      this.observer = observer;
      this.committer = committer;
    }

    @Override
    public boolean consumeWithConfirm(
        @Nullable StreamElement element,
        TopicPartition tp, long offset,
        Consumer<Throwable> errorHandler) {

      if (element != null) {
        return observer.onNext(element, new LogObserver.OffsetContext() {
          @Override
          public void commit(boolean succ, Throwable exc) {
            if (succ) {
              committer.commit(tp, offset);
            } else {
              errorHandler.accept(exc);
            }
          }

          @Override
          public Offset getCurrentOffset() {
            return () -> () -> tp.partition();
          }

        });
      } else {
        committer.commit(tp, offset);
      }
      return true;
    }
  }

  static final class BulkConsumer implements ElementConsumer {
    final BulkLogObserver observer;
    final QueryableTopicPartitionCommitter committer;

    BulkConsumer(BulkLogObserver observer, QueryableTopicPartitionCommitter committer) {
      this.observer = observer;
      this.committer = committer;
    }

    @Override
    public boolean consumeWithConfirm(
        @Nullable StreamElement element,
        TopicPartition tp, long offset,
        Consumer<Throwable> errorHandler) {

      if (element != null) {
        return observer.onNext(element, tp::partition, bulkCommitter(tp, offset, errorHandler));
      }
      return true;
    }

    private BulkLogObserver.OffsetContext bulkCommitter(
        TopicPartition tp, long offset, Consumer<Throwable> errorHandler) {

      return new BulkLogObserver.OffsetContext() {

        @Override
        public void commit(boolean success, Throwable err) {
          if (success) {
            committer.commit(tp, offset);
          } else {
            errorHandler.accept(err);
          }
        }

        @Override
        public List<Offset> getCommittedOffsets() {
          return committer.getCommittedOffsets();
        }
      };
    }
  }

}
