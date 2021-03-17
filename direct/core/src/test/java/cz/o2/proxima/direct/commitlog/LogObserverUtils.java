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
package cz.o2.proxima.direct.commitlog;

import cz.o2.proxima.functional.Consumer;
import cz.o2.proxima.functional.UnaryPredicate;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.util.List;

public class LogObserverUtils {

  public static LogObserver toList(List<StreamElement> list, Consumer<Boolean> onFinished) {
    return toList(list, onFinished, ign -> true);
  }

  public static LogObserver toList(
      List<StreamElement> list,
      Consumer<Boolean> onFinished,
      UnaryPredicate<StreamElement> shouldContinue) {

    return new LogObserver() {
      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

      @Override
      public boolean onNext(StreamElement ingest, OnNextContext context) {
        list.add(ingest);
        context.confirm();
        return shouldContinue.apply(ingest);
      }

      @Override
      public void onCompleted() {
        onFinished.accept(true);
      }

      @Override
      public void onCancelled() {
        onFinished.accept(false);
      }
    };
  }

  public static <T> LogObserver toList(List<T> list, AttributeDescriptor<T> attribute) {
    return (ingest, context) -> {
      if (ingest.getAttributeDescriptor().equals(attribute)) {
        attribute.valueOf(ingest).ifPresent(list::add);
      }
      context.confirm();
      return true;
    };
  }
}
