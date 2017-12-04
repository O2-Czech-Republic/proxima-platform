/**
 * Copyright 2017 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage.hbase;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.randomaccess.KeyValue;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;

/**
 * {@code RandomAccessReader} for HBase.
 */
public class RandomHBaseReader extends HBaseClientWrapper implements RandomAccessReader {

  private static class StringOffset implements Offset {
    private final String off;
    StringOffset(String off) {
      this.off = off;
    }
  }

  private final EntityDescriptor entity;

  public RandomHBaseReader(URI uri, Configuration conf,
      Map<String, Object> cfg,
      EntityDescriptor entity) {

    super(uri, conf, cfg);
    this.entity = entity;
  }

  @Override
  public Offset fetchOffset(Listing type, String key) {
    return new StringOffset(key);
  }

  @Override
  public Optional<KeyValue<?>> get(
      String key, String attribute, AttributeDescriptor<?> desc) {

    ensureClient();
    byte[] qualifier = attribute.getBytes();
    Get get = new Get(key.getBytes());
    get.addColumn(family, qualifier);
    try {
      Result res = client.get(get);
      Cell cell = res.getColumnLatestCell(family, qualifier);
      return Optional.ofNullable(cell == null
          ? null
          : kv(desc, cell));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void scanWildcard(
      String key, AttributeDescriptor<?> wildcard, Offset offset,
      int limit, Consumer<KeyValue<?>> consumer) {

    try {
      ensureClient();
      StringOffset stroff = (StringOffset) offset;
      Get get = new Get(key.getBytes());
      get.addFamily(family);
      get.setFilter(new ColumnPrefixFilter(wildcard.toAttributePrefix().getBytes()));
      Scan scan = new Scan(get);
      scan.setBatch(limit);
      if (stroff != null) {
        scan.setFilter(new ColumnPaginationFilter(limit, stroff.off.getBytes()));
      }

      int accepted = 0;
      try (ResultScanner scanner = client.getScanner(scan)) {
        Result next;
        while (accepted < limit && (next = scanner.next()) != null) {
          CellScanner cellScanner = next.cellScanner();
          while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            consumer.accept(kv(wildcard, cell));
            if (++accepted >= limit) {
              break;
            }
          }
        }
      }

    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void listEntities(
      Offset offset,
      int limit,
      Consumer<Pair<Offset, String>> consumer) {

  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  private KeyValue kv(AttributeDescriptor<?> desc, Cell cell) {
    String key = new String(
        cell.getRowArray(),
        cell.getRowOffset(), cell.getRowLength());
    String attribute = new String(
        cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());

    return KeyValue.of(entity, desc, key,
        attribute, new StringOffset(attribute), cell.getValue());
  }

}
