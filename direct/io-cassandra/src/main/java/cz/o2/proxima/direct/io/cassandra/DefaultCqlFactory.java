/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.io.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import cz.o2.proxima.io.serialization.proto.Serialization;
import cz.o2.proxima.io.serialization.proto.Serialization.Cell;
import cz.o2.proxima.io.serialization.shaded.com.google.protobuf.ByteString;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link CqlFactory} used by default. The default behavior is to take name of the attribute and
 * change it from camelCase to underscore_case. The cassandra URI looks like this:
 *
 * <pre>{@code
 * cassandra://<authority>/<table>/?primary=<primaryField>
 *     &secondary=<secondaryKeyField>&data=<dataField>&reversed=true*
 * }</pre>
 *
 * where: * primaryField is the column where primary key (or first part of composite key) is stored
 * * secondaryField is the second part of composite key (optional) * dataField is the name of data
 * field for wildcard attributes (optional) * reversed might by {@code true} if the composite key is
 * sorted in descending order
 */
@Slf4j
public class DefaultCqlFactory extends CacheableCqlFactory {

  @Internal
  interface Serializer extends Serializable {
    byte[] asCellBytes(StreamElement element);

    <T> KeyValue<T> fromCellBytes(
        EntityDescriptor entityDescriptor,
        AttributeDescriptor<T> attributeDescriptor,
        String key,
        String attribute,
        long stamp,
        RandomOffset offset,
        byte[] serializedValue);
  }

  private static class RawValueSerializer implements Serializer {

    @Override
    public byte[] asCellBytes(StreamElement element) {
      return element.getValue();
    }

    @Override
    public <T> KeyValue<T> fromCellBytes(
        EntityDescriptor entityDescriptor,
        AttributeDescriptor<T> attributeDescriptor,
        String key,
        String attribute,
        long stamp,
        RandomOffset offset,
        byte[] serializedValue) {

      return KeyValue.of(
          entityDescriptor, attributeDescriptor, key, attribute, offset, serializedValue, stamp);
    }
  }

  private static class V2Serializer implements Serializer {

    @Override
    public byte[] asCellBytes(StreamElement element) {
      return Serialization.Cell.newBuilder()
          .setValue(ByteString.copyFrom(element.getValue()))
          .setSeqId(element.hasSequentialId() ? element.getSequentialId() : 0)
          .build()
          .toByteArray();
    }

    @Override
    public <T> KeyValue<T> fromCellBytes(
        EntityDescriptor entityDescriptor,
        AttributeDescriptor<T> attributeDescriptor,
        String key,
        String attribute,
        long stamp,
        RandomOffset offset,
        byte[] serializedValue) {

      Cell cell = ExceptionUtils.uncheckedFactory(() -> Cell.parseFrom(serializedValue));
      if (cell.getSeqId() > 0) {
        return KeyValue.of(
            entityDescriptor,
            attributeDescriptor,
            cell.getSeqId(),
            key,
            attribute,
            offset,
            cell.getValue().toByteArray(),
            stamp);
      }
      return KeyValue.of(
          entityDescriptor,
          attributeDescriptor,
          key,
          attribute,
          offset,
          cell.getValue().toByteArray(),
          stamp);
    }
  }

  /** The name of the field used as primary key or first part of composite primary key. */
  String primaryField;

  /**
   * An optional name of secondary field for composite key. If missing, name of the attribute
   * (without .*) is used.
   */
  @Nullable String secondaryField;

  /**
   * Converter between type stored in cassandra and string used as a specifier in wildcard
   * attributes.
   */
  StringConverter<?> converter = StringConverter.getDefault();

  /** Serializer of {@link StreamElement} to cell and back. */
  private Serializer serializer = new RawValueSerializer();

  /** {@code true} if the secondary key sorting reversed (DESC). */
  boolean reversed = false;

  @Override
  protected void setup(Map<String, String> query, StringConverter<?> converter) {
    primaryField = query.get("primary");
    if (primaryField == null) {
      throw new IllegalArgumentException(
          "Query does not contain `primary' "
              + "parameter in query. This parameter specifies name of the "
              + "field that is being used as primary key (or first part "
              + "of a composite key).");
    }
    reversed =
        Optional.ofNullable(query.get("reversed"))
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(false);
    secondaryField = query.get("secondary");
    this.converter = converter;
    String serializerVersion =
        Optional.ofNullable(query.get("serializer")).map(Object::toString).orElse("v1");
    Preconditions.checkArgument(
        serializerVersion.equals("v1") || serializerVersion.equals("v2"),
        "Unknown serializer %s, supported only v1 or v2",
        serializerVersion);
    this.serializer = serializerVersion.equals("v2") ? new V2Serializer() : this.serializer;
  }

  @Override
  public Optional<BoundStatement> getWriteStatement(StreamElement element, Session session) {

    ensureSession(session);
    if (element.isDelete()) {
      return elementDelete(element);
    }

    return elementInsert(element);
  }

  @Override
  public BoundStatement getReadStatement(
      String key, String attribute, AttributeDescriptor<?> desc, Session session) {

    ensureSession(session);
    PreparedStatement statement = getPreparedGetStatement(session, attribute, desc);
    if (desc.isWildcard()) {
      return statement.bind(key, toColVal(attribute));
    }

    return statement.bind(key);
  }

  @Override
  public BoundStatement getListStatement(
      String key,
      AttributeDescriptor<?> wildcard,
      @Nullable Offsets.Raw offset,
      int limit,
      Session session) {

    ensureSession(session);
    PreparedStatement statement = getPreparedListStatement(session, wildcard);
    Object startVal = null;
    if (offset != null) {
      startVal = toColVal(offset.getRaw());
    }
    if (startVal == null) {
      startVal = reversed ? converter.max() : converter.min();
    }
    return statement.bind(key, startVal, limit < 0 ? Integer.MAX_VALUE : limit);
  }

  private Optional<BoundStatement> elementInsert(StreamElement ingest) {
    PreparedStatement prepared = getPreparedStatement(current, ingest);
    if (ingest.getAttributeDescriptor().isWildcard()) {
      String attr = ingest.getAttribute();
      Object colVal = toColVal(attr);
      if (colVal != null) {
        BoundStatement bind =
            prepared.bind(
                ingest.getKey(),
                colVal,
                ByteBuffer.wrap(serializeValue(ingest)),
                ingest.getStamp() * 1000L);
        return Optional.of(bind);
      }
      return Optional.empty();
    }

    BoundStatement bind =
        prepared.bind(
            ingest.getKey(), ByteBuffer.wrap(serializeValue(ingest)), ingest.getStamp() * 1000L);
    return Optional.of(bind);
  }

  private Optional<BoundStatement> elementDelete(StreamElement ingest) {
    PreparedStatement prepared = getPreparedStatement(current, ingest);
    if (!ingest.isDeleteWildcard() && ingest.getAttributeDescriptor().isWildcard()) {
      String attr = ingest.getAttribute();
      Object colVal = toColVal(attr);
      return Optional.of(prepared.bind(ingest.getStamp() * 1000L, colVal, ingest.getKey()));
    }
    return Optional.of(prepared.bind(ingest.getStamp() * 1000L, ingest.getKey()));
  }

  @Override
  protected String createInsertStatement(StreamElement element) {

    if (element.getAttributeDescriptor().isWildcard()) {
      // use the first part of the attribute name
      String colName = toColName(element.getAttributeDescriptor());
      return String.format(
          "INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?) USING TIMESTAMP ?%s",
          getTableName(),
          primaryField,
          toUnderScore(colName),
          toPayloadCol(element.getAttributeDescriptor()),
          ttl > 0 ? (" AND TTL " + ttl) : "");
    } else {
      return String.format(
          "INSERT INTO %s (%s, %s) VALUES (?, ?) USING TIMESTAMP ?%s",
          getTableName(),
          primaryField,
          toUnderScore(element.getAttribute()),
          ttl > 0 ? (" AND TTL " + ttl) : "");
    }
  }

  @Override
  protected String createDeleteStatement(StreamElement element) {
    if (element.getAttributeDescriptor().isWildcard()) {
      // use the first part of the attribute name
      String colName = toColName(element.getAttributeDescriptor());
      return String.format(
          "DELETE %s FROM %s USING TIMESTAMP ? WHERE %s=? AND %s=?",
          toPayloadCol(element.getAttributeDescriptor()),
          getTableName(),
          toUnderScore(colName),
          primaryField);
    } else {
      return String.format(
          "DELETE %s FROM %s USING TIMESTAMP ? WHERE %s=?",
          toUnderScore(element.getAttribute()), getTableName(), primaryField);
    }
  }

  @Override
  protected String createDeleteWildcardStatement(StreamElement what) {
    return String.format(
        "DELETE FROM %s USING TIMESTAMP ? WHERE %s=?", getTableName(), primaryField);
  }

  @Override
  protected String createGetStatement(String attribute, AttributeDescriptor<?> desc) {

    if (desc.isWildcard()) {
      String colName = toColName(desc);
      return String.format(
          "SELECT %s FROM %s WHERE %s=? AND %s=?",
          toPayloadCol(desc), getTableName(), primaryField, toUnderScore(colName));
    }

    return String.format(
        "SELECT %s FROM %s WHERE %s=?", toUnderScore(attribute), getTableName(), primaryField);
  }

  @Override
  protected String createListStatement(AttributeDescriptor<?> attr) {

    String colName = toColName(attr);
    String dataCol = toUnderScore(colName);
    return String.format(
        "SELECT %s, %s FROM %s WHERE %s=? AND %s%s? LIMIT ?",
        dataCol, toPayloadCol(attr), getTableName(), primaryField, dataCol, reversed ? "<" : ">");
  }

  private byte[] serializeValue(StreamElement ingest) {
    return serializer.asCellBytes(ingest);
  }

  private String toColName(AttributeDescriptor<?> desc) {
    if (secondaryField == null) {
      return desc.toAttributePrefix(false);
    }
    return secondaryField;
  }

  private @Nullable Object toColVal(String attr) {
    int dotPos = attr.indexOf('.');
    String colVal = "";
    if (dotPos > 0 && dotPos < attr.length() - 1) {
      colVal = attr.substring(dotPos + 1);
    }
    return converter.fromString(colVal);
  }

  @Override
  protected String createListEntitiesStatement() {
    return String.format(
        "SELECT %s, token(%s) FROM %s WHERE token(%s) > ? LIMIT ?",
        primaryField, primaryField, getTableName(), primaryField);
  }

  @Override
  protected String createFetchTokenStatement() {
    return String.format(
        "SELECT token(%s) FROM %s WHERE %s=?", primaryField, getTableName(), primaryField);
  }

  @Override
  protected String createListAllStatement(Session session) {
    throw new UnsupportedOperationException(
        "Unsupported. " + "See https://github.com/O2-Czech-Republic/proxima-platform/issues/67");
  }

  @Override
  public Statement scanPartition(
      List<AttributeDescriptor<?>> attributes, CassandraPartition partition, Session session) {

    StringBuilder columns = new StringBuilder();
    String comma = "";
    for (AttributeDescriptor<?> a : attributes) {
      columns.append(comma);
      columns.append(toUnderScore(toColName(a)));
      comma = ", ";
      if (a.isWildcard()) {
        columns.append(comma);
        columns.append(toUnderScore(toPayloadCol(a)));
      }
      comma = ", ";
    }
    String query =
        String.format(
            "SELECT %s, %s FROM %s WHERE token(%s) >= %d AND token(%s) %s %d",
            primaryField,
            columns,
            getTableName(),
            primaryField,
            partition.getTokenStart(),
            primaryField,
            partition.isEndInclusive() ? "<=" : "<",
            partition.getTokenEnd());

    log.info("Scanning partition with query {}", query);
    return new SimpleStatement(query);
  }

  @Override
  public <T> KeyValue<T> toKeyValue(
      EntityDescriptor entityDescriptor,
      AttributeDescriptor<T> attributeDescriptor,
      String key,
      String attribute,
      long stamp,
      RandomOffset offset,
      byte[] serializedValue) {

    return serializer.fromCellBytes(
        entityDescriptor, attributeDescriptor, key, attribute, stamp, offset, serializedValue);
  }

  @Override
  public <T> KvIterable<T> getListAllStatement(
      String key, Offsets.Raw offset, int limit, Session session) {
    throw new UnsupportedOperationException(
        "Unsupported. See https://github.com/O2-Czech-Republic/proxima-platform/issues/67");
  }
}
