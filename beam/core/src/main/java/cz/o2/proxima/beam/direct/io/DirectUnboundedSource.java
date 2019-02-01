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
package cz.o2.proxima.beam.direct.io;

import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * An {@link UnboundedSource} created from direct operator's {@link CommitLogReader}.
 */
class DirectUnboundedSource
    extends UnboundedSource<StreamElement, DirectUnboundedSource.Checkpoint> {

  static DirectUnboundedSource of(CommitLogReader reader) {
    return new DirectUnboundedSource(reader);
  }

  private final CommitLogReader reader;

  static class Checkpoint implements UnboundedSource.CheckpointMark {

    public Checkpoint() {
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  DirectUnboundedSource(CommitLogReader reader) {
    this.reader = reader;
  }

  @Override
  public List<? extends UnboundedSource<StreamElement, Checkpoint>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public UnboundedReader<StreamElement> createReader(
      PipelineOptions po, Checkpoint cmt) throws IOException {

    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Coder<Checkpoint> getCheckpointMarkCoder() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
