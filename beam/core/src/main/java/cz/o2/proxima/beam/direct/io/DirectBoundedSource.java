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
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * An {@link BoundedSource} created from direct operator's {@link CommitLogReader}.
 */
@Slf4j
class DirectBoundedSource extends AbstractDirectBoundedSource {

  static DirectBoundedSource of(
      RepositoryFactory factory, String name,
      CommitLogReader reader, Position position, long limit) {

    return new DirectBoundedSource(factory, name, reader, position, limit, null);
  }

  private final String name;
  private final CommitLogReader reader;
  private final Position position;
  private final long limit;
  private final Partition partition;

  DirectBoundedSource(
      RepositoryFactory factory,
      String name,
      CommitLogReader reader,
      Position position,
      long limit,
      @Nullable Partition partition) {

    super(factory);
    this.name = name;
    this.reader = Objects.requireNonNull(reader);
    this.position = position;
    this.limit = limit;
    this.partition = partition;
  }

  @Override
  public List<BoundedSource<StreamElement>> split(
      long desiredBundleSizeBytes, PipelineOptions opts) throws Exception {

    if (partition != null) {
      return Arrays.asList(this);
    }
    List<BoundedSource<StreamElement>> ret = new ArrayList<>();
    List<Partition> partitions = reader.getPartitions();
    for (Partition p : partitions) {
      ret.add(new DirectBoundedSource(
          factory, name, reader, position, limit / partitions.size(), p));
    }
    log.debug("Split source {} into {}", this, ret);
    return ret;
  }


  @Override
  public BoundedReader<StreamElement> createReader(
      PipelineOptions options) throws IOException {

    log.debug(
        "Creating reader reading from position {} on partition {}",
        position,
        partition);

    return BeamCommitLogReader.bounded(
        this, name, reader, position, limit, partition);
  }


}
