/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.tools.groovy;

import com.google.auto.service.AutoService;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;

@AutoService(StreamProvider.class)
public class DummyStreamProvider implements StreamProvider {

  @Override
  public void close() {}

  @Override
  public Stream<StreamElement> getStream(
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    throw new UnsupportedOperationException();
  }

  @Override
  public WindowedStream<StreamElement> getBatchUpdates(
      long startStamp,
      long endStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    throw new UnsupportedOperationException();
  }

  @Override
  public WindowedStream<StreamElement> getBatchSnapshot(
      long fromStamp,
      long toStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    throw new UnsupportedOperationException();
  }
}
