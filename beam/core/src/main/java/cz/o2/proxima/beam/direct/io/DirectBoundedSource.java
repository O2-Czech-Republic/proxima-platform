/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/** An {@link BoundedSource} created from direct operator's {@link CommitLogReader}. */
@Slf4j
class DirectBoundedSource extends AbstractDirectBoundedSource {

  private static final long serialVersionUID = 1L;

  static DirectBoundedSource of(
      RepositoryFactory factory,
      String name,
      CommitLogReader reader,
      Position position,
      long limit) {

    return new DirectBoundedSource(factory, name, reader, position, limit, null);
  }

  private final String name;
  private final CommitLogReader.Factory<?> readerFactory;
  private final Position position;
  private final long limit;
  private final Partition partition;
  private transient CommitLogReader reader;

  DirectBoundedSource(
      RepositoryFactory factory,
      String name,
      CommitLogReader reader,
      Position position,
      long limit,
      @Nullable Partition partition) {

    super(factory);
    this.name = name;
    this.readerFactory = Objects.requireNonNull(reader).asFactory();
    this.position = position;
    this.limit = limit;
    this.partition = partition;
    this.reader = reader;
  }

  @Override
  public List<BoundedSource<StreamElement>> split(
      long desiredBundleSizeBytes, PipelineOptions opts) {

    if (partition != null) {
      return Collections.singletonList(this);
    }
    List<Partition> partitions = reader().getPartitions();
    int numPartitions = partitions.size();
    List<BoundedSource<StreamElement>> ret =
        partitions
            .stream()
            .map(
                p ->
                    new DirectBoundedSource(
                        factory, name, reader(), position, limit / numPartitions, p))
            .collect(Collectors.toList());
    log.debug("Split source {} into {}", this, ret);
    return ret;
  }

  @Override
  public BoundedReader<StreamElement> createReader(PipelineOptions options) {
    log.debug("Creating reader reading from position {} on partition {}", position, partition);
    return BeamCommitLogReader.bounded(this, name, reader(), position, limit, partition);
  }

  private CommitLogReader reader() {
    if (reader == null) {
      reader = readerFactory.apply(factory.apply());
    }
    return reader;
  }
}
