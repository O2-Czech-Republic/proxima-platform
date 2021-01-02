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
package cz.o2.proxima.beam.core;

import cz.o2.proxima.repository.DataOperatorFactory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.transform.ContextualProxyTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

/** Transform applicable on proxied attributes in {@code apply} section. */
public abstract class BeamProxyTransform implements ContextualProxyTransform<BeamDataOperator> {

  public abstract PCollection<StreamElement> createStream(
      String name,
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      long limit);

  public abstract PCollection<StreamElement> createStreamFromUpdates(
      Pipeline pipeline, long startStamp, long endStamp, long limit);

  public abstract PCollection<StreamElement> createBatch(
      Pipeline pipeline, long startStamp, long endStamp);

  @Override
  public final boolean isDelegateOf(DataOperatorFactory operatorFactory) {
    return operatorFactory instanceof BeamDataOperatorFactory;
  }
}
