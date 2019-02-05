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

import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.LogObserver.OffsetCommitter;
import cz.o2.proxima.direct.commitlog.Offset;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.beam.repackaged.beam_sdks_java_extensions_kryo.com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * An {@link UnboundedSource} created from direct operator's {@link CommitLogReader}.
 */
class DirectUnboundedSource
    extends UnboundedSource<StreamElement, DirectUnboundedSource.Checkpoint> {

  static DirectUnboundedSource of(
      Repository repo, String name,
      CommitLogReader reader, Position position, long limit) {

    return new DirectUnboundedSource(repo, name, reader, position, limit, -1);
  }

  static class Checkpoint implements UnboundedSource.CheckpointMark, Serializable {

    @Getter
    @Nullable
    private final Offset offset;
    @Getter
    private final long limit;
    @Nullable
    private final transient OffsetCommitter committer;

    Checkpoint(Offset offset, long limit, OffsetCommitter committer) {
      this.offset = offset;
      this.limit = limit;
      this.committer = committer;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      if (committer != null) {
        committer.confirm();
      }
    }

  }

  private final Repository repo;
  private final String name;
  private final CommitLogReader reader;
  private final Position position;
  private final int partitions;
  private final long limit;
  private final int splitId;

  DirectUnboundedSource(
      Repository repo, String name, CommitLogReader reader,
      Position position, long limit, int splitId) {

    this.repo = repo;
    this.name = name;
    this.reader = reader;
    this.position = position;
    this.partitions = reader.getPartitions().size();
    this.limit = limit;
    this.splitId = splitId;
  }

  @Override
  public List<UnboundedSource<StreamElement, Checkpoint>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {

    if (splitId != -1) {
      return Arrays.asList(this);
    }
    List<UnboundedSource<StreamElement, Checkpoint>> ret = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      ret.add(new DirectUnboundedSource(
          repo, name, reader, position, limit / partitions, i));
    }
    return ret;
  }

  @Override
  public UnboundedReader<StreamElement> createReader(
      PipelineOptions po, Checkpoint cmt) throws IOException {

    Offset offset = cmt == null ? null : cmt.getOffset();
    long readerLimit = cmt == null ? limit : cmt.getLimit();
    return BeamCommitLogReader.unbounded(
        this, name, reader, position, readerLimit, splitId, offset);
  }

  @Override
  public Coder<Checkpoint> getCheckpointMarkCoder() {
    return KryoCoder.of(kryo ->
      kryo.addDefaultSerializer(Serializable.class, new JavaSerializer()));
  }

  @Override
  public Coder<StreamElement> getOutputCoder() {
    return StreamElementCoder.of(repo);
  }

  @Override
  public boolean requiresDeduping() {
    // when offsets are externalizable we have certainty that we can pause and
    // continue without duplicates
    return !reader.hasExternalizableOffsets();
  }

}
