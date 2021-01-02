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
package cz.o2.proxima.direct.pubsub;

import static org.mockito.Mockito.*;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import java.util.Objects;
import java.util.function.Consumer;

/** A mock {@link Publisher}. */
class MockPublisher {

  static Publisher create(String project, String topic, Consumer<PubsubMessage> writer) {
    Objects.requireNonNull(project);
    Objects.requireNonNull(topic);
    Objects.requireNonNull(writer);
    Publisher ret = mock(Publisher.class);
    doAnswer(
            invocation -> {
              writer.accept(invocation.getArgument(0, PubsubMessage.class));
              ApiFuture future = mock(ApiFuture.class);
              when(future.isDone()).thenReturn(true);
              when(future.isCancelled()).thenReturn(false);
              doAnswer(
                      i -> {
                        i.getArgument(0, Runnable.class).run();
                        return null;
                      })
                  .when(future)
                  .addListener(any(), any());
              return future;
            })
        .when(ret)
        .publish(any());
    return ret;
  }
}
