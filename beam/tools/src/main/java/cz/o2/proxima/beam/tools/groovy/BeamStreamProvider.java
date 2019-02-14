/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.tools.groovy;

import com.google.common.base.Preconditions;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.tools.groovy.Stream;
import cz.o2.proxima.tools.groovy.StreamProvider;
import cz.o2.proxima.tools.groovy.WindowedStream;
import org.apache.beam.sdk.Pipeline;

/**
 * A {@link StreamProvider} for groovy tools based on beam.
 */
public abstract class BeamStreamProvider implements StreamProvider {

  Repository repo;
  BeamDataOperator beam;

  @Override
  public void init(Repository repo, String[] args) {
    this.repo = repo;
    Preconditions.checkArgument(
        this.repo.hasOperator("beam"),
        "Please include proxima-beam-core dependency");
    this.beam = repo.getOrCreateOperator(BeamDataOperator.class);
  }

  @Override
  public Stream<StreamElement> getStream(
      Position position, boolean stopAtCurrent,
      boolean eventTime, TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    return BeamStream.stream(
        beam, position, stopAtCurrent, eventTime, terminateCheck,
        getPipelineFactory(), attrs);
  }

  @Override
  public WindowedStream<StreamElement> getBatchUpdates(
      long startStamp, long endStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    return BeamStream.batchUpdates(
        beam, startStamp, endStamp, terminateCheck, getPipelineFactory(), attrs);
  }

  @Override
  public WindowedStream<StreamElement> getBatchSnapshot(
      long fromStamp, long toStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs) {

    return BeamStream.batchSnapshot(
        beam, fromStamp, toStamp, terminateCheck, getPipelineFactory(), attrs);
  }

  @Override
  public void close() {
    if (beam != null) {
      beam.close();
    }
  }

  /**
   * Create factory to be used for pipeline creation.
   * @return the factory
   */
  protected abstract Factory<Pipeline> getPipelineFactory();

}
