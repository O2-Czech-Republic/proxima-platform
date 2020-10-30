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
package cz.o2.proxima.direct.hbase;

import static cz.o2.proxima.direct.hbase.Util.cloneArray;

import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.batch.BatchLogObservers;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.batch.ObserveHandle;
import cz.o2.proxima.direct.batch.TerminationContext;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.Partition;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.ExceptionUtils;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;

/** A {@link BatchLogReader} for HBase. */
@Slf4j
class HBaseLogReader extends HBaseClientWrapper implements BatchLogReader {

  private final EntityDescriptor entity;
  private final cz.o2.proxima.functional.Factory<ExecutorService> executorFactory;
  private final ExecutorService executor;

  public HBaseLogReader(
      URI uri,
      Configuration conf,
      EntityDescriptor entity,
      cz.o2.proxima.functional.Factory<ExecutorService> executorFactory) {

    super(uri, conf);
    this.entity = entity;
    this.executorFactory = executorFactory;
    this.executor = executorFactory.apply();
  }

  @Override
  public List<Partition> getPartitions(long startStamp, long endStamp) {
    try {
      ensureClient();
      List<Partition> ret = new ArrayList<>();
      byte[][] end = conn.getRegionLocator(tableName()).getEndKeys();
      byte[] startPos = new byte[0];
      if (startStamp < 0) {
        startStamp = 0;
      }
      for (int i = 0; i < end.length; i++) {
        ret.add(new HBasePartition(i, startPos, end[i], startStamp, endStamp));
        startPos = end[i];
      }
      return ret;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public ObserveHandle observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    TerminationContext terminationContext = new TerminationContext(observer);
    observeInternal(partitions, attributes, observer, terminationContext);
    return terminationContext.asObserveHandle();
  }

  public void observeInternal(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer,
      TerminationContext terminationContext) {

    executor.submit(
        () -> {
          terminationContext.setRunningThread();
          ensureClient();
          try {
            ExceptionUtils.ignoringInterrupted(
                () -> flushPartitions(partitions, attributes, terminationContext, observer));
          } catch (Throwable ex) {
            terminationContext.handleErrorCaught(
                ex,
                () -> {
                  log.info("Restarting processing by request");
                  observeInternal(partitions, attributes, observer, terminationContext);
                });
          }
        });
  }

  @Override
  public Factory<?> asFactory() {
    final URI uri = getUri();
    final EntityDescriptor entity = this.entity;
    final cz.o2.proxima.functional.Factory<ExecutorService> executorFactory = this.executorFactory;
    final byte[] serializedConf = this.serializedConf;
    return repo ->
        new HBaseLogReader(
            uri, deserialize(serializedConf, new Configuration()), entity, executorFactory);
  }

  private void flushPartitions(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      TerminationContext terminationContext,
      BatchLogObserver observer)
      throws IOException {

    outer:
    for (Partition p : partitions) {
      HBasePartition hp = (HBasePartition) p;
      Scan scan = new Scan(hp.getStartKey(), hp.getEndKey());
      scan.addFamily(family);
      scan.setTimeRange(hp.getStartStamp(), hp.getEndStamp());
      scan.setFilter(toFilter(attributes));

      try (ResultScanner scanner = client.getScanner(scan)) {
        Result next = null;
        do {
          if (terminationContext.isCancelled()
              || next != null && !consume(next, attributes, hp, observer)) {
            break outer;
          }
        } while ((next = scanner.next()) != null);
      }
    }
    terminationContext.finished();
  }

  private boolean consume(
      Result r, List<AttributeDescriptor<?>> attrs, HBasePartition hp, BatchLogObserver observer)
      throws IOException {

    CellScanner scanner = r.cellScanner();
    while (scanner.advance()) {
      if (!observer.onNext(
          toStreamElement(scanner.current(), attrs, hp), BatchLogObservers.defaultContext(hp))) {
        return false;
      }
    }
    return true;
  }

  private StreamElement toStreamElement(
      Cell cell, List<AttributeDescriptor<?>> attrs, HBasePartition hp) {

    String key = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());

    String qualifier =
        new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

    for (AttributeDescriptor<?> d : attrs) {
      if (qualifier.startsWith(d.toAttributePrefix())) {
        return StreamElement.upsert(
            entity,
            d,
            new String(hp.getStartKey()) + "#" + cell.getSequenceId(),
            key,
            qualifier,
            cell.getTimestamp(),
            cloneArray(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
      }
    }
    throw new IllegalStateException("Illegal state! Fix code!");
  }

  // build filter for specified attributes and stamps
  private Filter toFilter(List<AttributeDescriptor<?>> attributes) {

    // OR filter
    FilterList attrFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    attributes.forEach(
        attr -> {
          if (attr.isWildcard()) {
            attrFilter.addFilter(
                new ColumnPrefixFilter(attr.toAttributePrefix().getBytes(StandardCharsets.UTF_8)));
          } else {
            attrFilter.addFilter(
                new QualifierFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(attr.getName().getBytes(StandardCharsets.UTF_8))));
          }
        });

    return attrFilter;
  }

  @Override
  public int hashCode() {
    return 73;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof HBaseLogReader) {
      HBaseLogReader o = (HBaseLogReader) obj;
      return o.entity.equals(entity) && o.uri.equals(uri);
    }
    return false;
  }
}
