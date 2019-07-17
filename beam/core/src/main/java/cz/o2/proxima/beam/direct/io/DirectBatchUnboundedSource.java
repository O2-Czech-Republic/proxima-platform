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

import com.google.common.collect.Lists;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * Source reading from {@link BatchLogObservable} in unbounded manner.
 */
@Slf4j
public class DirectBatchUnboundedSource
    extends UnboundedSource<StreamElement, DirectBatchUnboundedSource.Checkpoint> {

  public static DirectBatchUnboundedSource of(
      RepositoryFactory factory,
      BatchLogObservable reader,
      List<AttributeDescriptor<?>> attributes) {

    return new DirectBatchUnboundedSource(factory, reader, attributes);
  }

  public static class Checkpoint implements UnboundedSource.CheckpointMark, Serializable {

    @Getter
    private final List<Partition> partitions;
    @Getter
    private final long skipFromFirst;

    Checkpoint(List<Partition> partitions, long skipFromFirst) {
      this.partitions = Lists.newArrayList(partitions);
      this.skipFromFirst = skipFromFirst;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
      // nop
    }
  }

  /**
   * Use gzip to compress the serialized checkpoint, as it might easily grow
   * in size (batch partitions might contain many files).
   */
  public static class CheckpointCoder extends Coder<Checkpoint> {

    @Override
    public void encode(Checkpoint value, OutputStream outStream)
        throws CoderException, IOException {

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      GZIPOutputStream gzout = new GZIPOutputStream(baos);
      ObjectOutputStream oos = new ObjectOutputStream(gzout);
      oos.writeObject(value);
      oos.flush();
      gzout.finish();
      byte[] bytes = baos.toByteArray();
      DataOutputStream dos = new DataOutputStream(outStream);
      dos.writeInt(bytes.length);
      dos.write(bytes);
    }

    @Override
    public Checkpoint decode(InputStream inStream) throws CoderException, IOException {

      DataInputStream dis = new DataInputStream(inStream);
      int length = dis.readInt();
      byte[] bytes = new byte[length];
      dis.readFully(bytes);
      GZIPInputStream gzin = new GZIPInputStream(new ByteArrayInputStream(bytes));
      ObjectInputStream ois = new ObjectInputStream(gzin);
      try {
        return (Checkpoint) ois.readObject();
      } catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {

    }

  }

  final RepositoryFactory factory;
  final BatchLogObservable reader;
  final List<AttributeDescriptor<?>> attributes;
  final List<Partition> partitions;

  private DirectBatchUnboundedSource(
      RepositoryFactory factory,
      BatchLogObservable reader,
      List<AttributeDescriptor<?>> attributes) {

    this.factory = factory;
    this.reader = reader;
    this.attributes = Collections.unmodifiableList(attributes);
    this.partitions = Collections.emptyList();
  }

  private DirectBatchUnboundedSource(
      DirectBatchUnboundedSource parent, List<Partition> partitions) {

    this.factory = parent.factory;
    this.reader = parent.reader;
    this.attributes = parent.attributes;
    List<Partition> parts = Lists.newArrayList(partitions);
    parts.sort(partitionsComparator());
    this.partitions = Collections.unmodifiableList(parts);
  }

  @Override
  public List<? extends UnboundedSource<StreamElement, Checkpoint>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {

    if (partitions.isEmpty()) {
      // round robin
      List<Partition> parts = reader.getPartitions();
      List<List<Partition>> splits = new ArrayList<>();
      int current = 0;
      for (Partition p : parts) {
        if (splits.size() <= current) {
          splits.add(new ArrayList<>());
        }
        splits.get(current).add(p);
        current = (current + 1) % desiredNumSplits;
      }
      return splits.stream()
          .map(s -> new DirectBatchUnboundedSource(this, s))
          .collect(Collectors.toList());
    }
    return Arrays.asList(this);
  }

  @Override
  public UnboundedReader<StreamElement> createReader(
      PipelineOptions options, Checkpoint checkpointMark) throws IOException {

    List<Partition> toProcess = Collections.synchronizedList(
        new ArrayList<>(checkpointMark == null
            ? partitions
            : checkpointMark.partitions));
    return new UnboundedReader<StreamElement>() {

      BlockingQueue<StreamElement> queue = new ArrayBlockingQueue<>(100);
      AtomicBoolean running = new AtomicBoolean();
      long consumedFromCurrent = 0;
      @Nullable StreamElement current = null;
      long skip = checkpointMark == null ? 0 : checkpointMark.skipFromFirst;
      Instant watermark;
      Partition runningPartition = null;

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        do {
          if (queue.isEmpty() && !running.get()) {
            if (runningPartition != null) {
              toProcess.remove(0);
              runningPartition = null;
            }
            if (!toProcess.isEmpty()) {
              // read partitions one by one
              runningPartition = toProcess.get(0);
              reader.observe(
                  Arrays.asList(runningPartition),
                  attributes, asObserver(queue, running));
              running.set(true);
              watermark = new Instant(runningPartition.getMinTimestamp());
              consumedFromCurrent = 0;
            } else {
              watermark = BoundedWindow.TIMESTAMP_MAX_VALUE;
              return false;
            }
          }
          try {
            current = queue.take();
            consumedFromCurrent++;
          } catch (InterruptedException ex) {
            log.warn("Interrupted while waiting for data.", ex);
            Thread.currentThread().interrupt();
            return false;
          }
        } while (skip-- > 0);
        return true;
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
        return DirectBatchUnboundedSource.this;
      }

      @Override
      public StreamElement getCurrent() throws NoSuchElementException {
        return current;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current == null) {
          return BoundedWindow.TIMESTAMP_MIN_VALUE;
        }
        return new Instant(current.getStamp());
      }

      @Override
      public void close() throws IOException {

      }

    };
  }

  @Override
  public Coder<Checkpoint> getCheckpointMarkCoder() {
    return new CheckpointCoder();
  }

  @Override
  public Coder<StreamElement> getOutputCoder() {
    return StreamElementCoder.of(factory);
  }

  private static Comparator<Partition> partitionsComparator() {
    return (p1, p2) -> {
      int cmp = Long.compare(p1.getMinTimestamp(), p2.getMinTimestamp());
      if (cmp == 0) {
        return Long.compare(p1.getMaxTimestamp(), p2.getMaxTimestamp());
      }
      return cmp;
    };
  }

  private static BatchLogObserver asObserver(
      BlockingQueue<StreamElement> queue,
      AtomicBoolean running) {

    return new BatchLogObserver() {

      @Override
      public boolean onNext(StreamElement element) {
        try {
          queue.put(element);
          return true;
        } catch (InterruptedException ex) {
          log.warn("Interrupted while reading data.", ex);
          Thread.currentThread().interrupt();
          return false;
        }
      }

      @Override
      public void onCompleted() {
        running.set(false);
      }

      @Override
      public boolean onError(Throwable error) {
        throw new RuntimeException(error);
      }

    };
  }



}
