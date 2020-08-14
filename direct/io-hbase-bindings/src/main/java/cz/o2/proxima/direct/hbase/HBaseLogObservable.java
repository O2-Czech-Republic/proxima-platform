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

import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.batch.BatchLogObserver;
import cz.o2.proxima.direct.core.Partition;
import cz.o2.proxima.functional.Factory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
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

/** A {@code BatchLogObservable} for HBase. */
@Slf4j
class HBaseLogObservable extends HBaseClientWrapper implements BatchLogObservable {

  private final EntityDescriptor entity;
  private final cz.o2.proxima.functional.Factory<Executor> executorFactory;
  private final Executor executor;

  public HBaseLogObservable(
      URI uri,
      Configuration conf,
      EntityDescriptor entity,
      cz.o2.proxima.functional.Factory<Executor> executorFactory) {

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
  public void observe(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer) {

    executor.execute(
        () -> {
          ensureClient();
          try {
            flushPartitions(partitions, attributes, observer);
          } catch (Throwable ex) {
            log.warn("Failed to observe partitions {}", partitions, ex);
            if (observer.onError(ex)) {
              log.info("Restaring processing by request");
              observe(partitions, attributes, observer);
            }
          }
        });
  }

  @Override
  public Factory<?> asFactory() {
    final URI uri = getUri();
    final EntityDescriptor entity = this.entity;
    final cz.o2.proxima.functional.Factory<Executor> executorFactory = this.executorFactory;
    final byte[] serializedConf = this.serializedConf;
    return repo ->
        new HBaseLogObservable(
            uri, deserialize(serializedConf, new Configuration()), entity, executorFactory);
  }

  private void flushPartitions(
      List<Partition> partitions,
      List<AttributeDescriptor<?>> attributes,
      BatchLogObserver observer)
      throws IOException {

    for (Partition p : partitions) {
      HBasePartition hp = (HBasePartition) p;
      Scan scan = new Scan(hp.getStartKey(), hp.getEndKey());
      scan.addFamily(family);
      scan.setTimeRange(hp.getStartStamp(), hp.getEndStamp());
      scan.setFilter(toFilter(attributes));

      boolean finish = false;
      try (ResultScanner scanner = client.getScanner(scan)) {
        Result next;
        while (((next = scanner.next()) != null) && !Thread.currentThread().isInterrupted()) {

          if (!consume(next, attributes, hp, observer)) {
            finish = true;
            break;
          }
        }
      }
      if (finish) {
        break;
      }
    }
    observer.onCompleted();
  }

  private boolean consume(
      Result r, List<AttributeDescriptor<?>> attrs, HBasePartition hp, BatchLogObserver observer)
      throws IOException {

    CellScanner scanner = r.cellScanner();
    while (scanner.advance()) {
      if (!observer.onNext(toStreamElement(scanner.current(), attrs, hp), hp)) {
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
    if (obj instanceof HBaseLogObservable) {
      HBaseLogObservable o = (HBaseLogObservable) obj;
      return o.entity.equals(entity) && o.uri.equals(uri);
    }
    return false;
  }
}
