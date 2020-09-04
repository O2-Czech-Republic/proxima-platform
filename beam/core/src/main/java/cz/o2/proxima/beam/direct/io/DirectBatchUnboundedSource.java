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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.beam.direct.io.DirectDataAccessorWrapper.ConfigReader;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ForwardingInputStream;
import cz.o2.proxima.util.ForwardingOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * Source reading from {@link BatchLogReader} in unbounded manner. The source can be configured
 * using repository config as follows:
 *
 * <pre>
 *   beam.unbounded-batch {
 *     my-source {
 *       uri = &lt;storageUri of batch updates attribute family&gt;
 *       throughput = &lt;throughput in bytes per second per reader&gt;
 *     }
 *   }
 * </pre>
 */
@Slf4j
public class DirectBatchUnboundedSource
    extends UnboundedSource<StreamElement, DirectBatchUnboundedSource.Checkpoint> {

  private static final long serialVersionUID = 1L;

  public static DirectBatchUnboundedSource of(
      RepositoryFactory factory,
      BatchLogReader reader,
      ConfigReader configReader,
      List<AttributeDescriptor<?>> attrs,
      long startStamp,
      long endStamp) {

    return new DirectBatchUnboundedSource(
        factory, reader, configReader, attrs, startStamp, endStamp);
  }

  @ToString
  public static class Checkpoint implements UnboundedSource.CheckpointMark, Serializable {

    private static final long serialVersionUID = 1L;

    @Getter private final List<Partition> partitions;
    @Getter private final long skipFromFirst;

    Checkpoint(List<Partition> partitions, long skipFromFirst) {
      this.partitions = Lists.newArrayList(partitions);
      this.skipFromFirst = skipFromFirst;
    }

    @Override
    public void finalizeCheckpoint() {
      // nop
    }
  }

  /**
   * Use gzip to compress the serialized checkpoint, as it might easily grow in size (batch
   * partitions might contain many files).
   */
  public static class CheckpointCoder extends Coder<Checkpoint> {

    private static final SerializableCoder<Checkpoint> DELEGATE =
        SerializableCoder.of(Checkpoint.class);

    private static final long serialVersionUID = 1L;

    @Override
    public void encode(Checkpoint value, OutputStream outStream) throws IOException {
      try (final GZIPOutputStream gzipOutputStream =
          new GZIPOutputStream(
              new ForwardingOutputStream(outStream) {

                @Override
                public void close() {
                  // No-op. We don't want gzip output stream to close underlying stream.
                }
              })) {
        DELEGATE.encode(value, gzipOutputStream);
      }
    }

    @Override
    public Checkpoint decode(InputStream inStream) throws IOException {
      try (final GZIPInputStream gzipInputStream =
          new GZIPInputStream(
              new ForwardingInputStream(inStream) {

                @Override
                public void close() {

                  // No-op. We don't want gzip input stream to close underlying stream.
                }
              })) {
        return DELEGATE.decode(gzipInputStream);
      }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      DELEGATE.verifyDeterministic();
    }
  }

  private final RepositoryFactory repositoryFactory;
  private final BatchLogReader.Factory<?> readerFactory;
  private final List<AttributeDescriptor<?>> attributes;
  private final List<Partition> partitions;
  private final long startStamp;
  private final long endStamp;
  private final ConfigReader configReader;
  private transient @Nullable BatchLogReader reader;
  private transient long maxThroughput;

  private DirectBatchUnboundedSource(
      RepositoryFactory repositoryFactory,
      BatchLogReader reader,
      ConfigReader configReader,
      List<AttributeDescriptor<?>> attributes,
      long startStamp,
      long endStamp) {

    this.repositoryFactory = repositoryFactory;
    this.readerFactory = reader.asFactory();
    this.attributes = Collections.unmodifiableList(attributes);
    this.partitions = Collections.emptyList();
    this.startStamp = startStamp;
    this.endStamp = endStamp;
    this.configReader = configReader;
    this.reader = reader;
  }

  private DirectBatchUnboundedSource(
      DirectBatchUnboundedSource parent,
      List<Partition> partitions,
      long startStamp,
      long endStamp) {

    this.repositoryFactory = parent.repositoryFactory;
    this.readerFactory = parent.readerFactory;
    this.attributes = parent.attributes;
    this.startStamp = startStamp;
    this.endStamp = endStamp;
    List<Partition> parts = Lists.newArrayList(partitions);
    parts.sort(partitionsComparator());
    this.partitions = Collections.unmodifiableList(parts);
    if (log.isDebugEnabled()) {
      log.debug(
          "Created source with partition min timestamps {}",
          parts.stream().map(Partition::getMinTimestamp).collect(Collectors.toList()));
    }
    this.configReader = parent.configReader;
  }

  @Override
  public List<? extends UnboundedSource<StreamElement, Checkpoint>> split(
      int desiredNumSplits, PipelineOptions options) {

    if (partitions.isEmpty()) {
      // round robin
      List<Partition> parts = reader().getPartitions(startStamp, endStamp);
      List<List<Partition>> splits = new ArrayList<>();
      int current = 0;
      for (Partition p : parts) {
        if (splits.size() <= current) {
          splits.add(new ArrayList<>());
        }
        splits.get(current).add(p);
        current = (current + 1) % desiredNumSplits;
      }
      return splits
          .stream()
          .map(s -> new DirectBatchUnboundedSource(this, s, startStamp, endStamp))
          .collect(Collectors.toList());
    }
    return Collections.singletonList(this);
  }

  @Override
  public UnboundedReader<StreamElement> createReader(
      PipelineOptions options, Checkpoint checkpointMark) {

    List<Partition> toProcess =
        Collections.synchronizedList(
            new ArrayList<>(checkpointMark == null ? partitions : checkpointMark.partitions));
    return new StreamElementUnboundedReader(
        DirectBatchUnboundedSource.this, reader(), attributes, checkpointMark, toProcess);
  }

  private BatchLogReader reader() {
    if (reader == null) {
      reader = readerFactory.apply(repositoryFactory.apply());
    }
    return reader;
  }

  @Override
  public Coder<Checkpoint> getCheckpointMarkCoder() {
    return new CheckpointCoder();
  }

  @Override
  public Coder<StreamElement> getOutputCoder() {
    return StreamElementCoder.of(repositoryFactory);
  }

  @VisibleForTesting
  static Comparator<Partition> partitionsComparator() {
    return (p1, p2) -> {
      int cmp = Long.compare(p1.getMinTimestamp(), p2.getMinTimestamp());
      if (cmp == 0) {
        return Long.compare(p1.getMaxTimestamp(), p2.getMaxTimestamp());
      }
      return cmp;
    };
  }

  private long getMaxThroughput() {
    if (maxThroughput == 0) {
      long configured = configReader.getBytesPerSecThroughput(repositoryFactory.apply());
      Preconditions.checkArgument(configured != 0, "Max bytes per sec cannot be 0");
      maxThroughput = configured;
    }
    return maxThroughput;
  }

  private static class StreamElementUnboundedReader extends UnboundedReader<StreamElement> {

    private final DirectBatchUnboundedSource source;
    private final BatchLogReader reader;
    private final List<AttributeDescriptor<?>> attributes;
    private final List<Partition> toProcess;
    private final long createdTime = System.currentTimeMillis();
    private BlockingQueueLogObserver observer;

    long consumedFromCurrent;
    @Nullable StreamElement current = null;
    long skip;
    Instant watermark = Instant.ofEpochMilli(Long.MIN_VALUE);
    @Nullable Partition runningPartition = null;
    long bytesConsumed = 0L;

    public StreamElementUnboundedReader(
        DirectBatchUnboundedSource source,
        BatchLogReader reader,
        List<AttributeDescriptor<?>> attributes,
        @Nullable Checkpoint checkpointMark,
        List<Partition> toProcess) {

      this.source = Objects.requireNonNull(source);
      this.reader = Objects.requireNonNull(reader);
      this.attributes = new ArrayList<>(Objects.requireNonNull(attributes));
      this.toProcess = new ArrayList<>(Objects.requireNonNull(toProcess));
      this.consumedFromCurrent = 0;
      this.skip = checkpointMark == null ? 0 : checkpointMark.skipFromFirst;
      log.info(
          "Created {} reading from {} with max throughput {}",
          getClass().getSimpleName(),
          reader,
          source.getMaxThroughput());
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (exceededThroughput()) {
        return false;
      }
      do {
        if (observer == null && !startNewObserver()) {
          return false;
        }
        try {
          current = observer.takeBlocking();
        } catch (InterruptedException ex) {
          log.debug("Interrupted while reading data", ex);
          observer.stop();
          Thread.currentThread().interrupt();
        }
        if (current == null) {
          Throwable error = observer.getError();
          if (error != null) {
            throw new IOException(error);
          }
          observer = null;
          return false;
        }
        consumedFromCurrent++;
      } while (skip-- > 0);
      bytesConsumed += sizeOf(current);
      return true;
    }

    private boolean startNewObserver() {
      if (runningPartition != null) {
        toProcess.remove(0);
        runningPartition = null;
      }
      if (!toProcess.isEmpty()) {
        // read partitions one by one
        runningPartition = toProcess.get(0);
        observer = newObserver(runningPartition);
        reader.observe(Collections.singletonList(runningPartition), attributes, observer);
        watermark = new Instant(runningPartition.getMinTimestamp());
        consumedFromCurrent = 0;
      } else {
        watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
        return false;
      }
      return true;
    }

    private BlockingQueueLogObserver newObserver(Partition partition) {
      return BlockingQueueLogObserver.create(
          "DirectBatchUnbounded:" + partition.getId(), watermark.getMillis());
    }

    private static int sizeOf(StreamElement element) {
      return (element.isDelete() ? 0 : element.getValue().length)
          + element.getKey().length()
          + element.getAttribute().length()
          + element.getUuid().length();
    }

    private boolean exceededThroughput() {
      if (source.getMaxThroughput() < 0) {
        return false;
      }
      long now = System.currentTimeMillis();
      long durationMillis = now - createdTime;
      return bytesConsumed > durationMillis * source.getMaxThroughput() / 1000;
    }

    @Override
    public Instant getWatermark() {
      return watermark;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new Checkpoint(toProcess, consumedFromCurrent);
    }

    @Override
    public UnboundedSource<StreamElement, ?> getCurrentSource() {
      return source;
    }

    @Override
    public StreamElement getCurrent() throws NoSuchElementException {
      return Objects.requireNonNull(current);
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (current == null) {
        return BoundedWindow.TIMESTAMP_MIN_VALUE;
      }
      return new Instant(current.getStamp());
    }

    @Override
    public void close() {
      Optional.ofNullable(observer).ifPresent(BlockingQueueLogObserver::stop);
    }
  }
}
