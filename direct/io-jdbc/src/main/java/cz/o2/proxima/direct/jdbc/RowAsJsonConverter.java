/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.jdbc;

import cz.o2.proxima.core.repository.AttributeDescriptor;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import cz.o2.proxima.internal.com.google.gson.JsonObject;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

public class RowAsJsonConverter implements Converter<String> {

  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS+00");

  private static final ZoneId UTC = ZoneId.of("UTC");

  @FunctionalInterface
  private interface JsonFormatter {
    void addToObject(String columnName, ResultSet result, JsonObject obj, int pos)
        throws SQLException;
  }

  private static final Map<Integer, JsonFormatter> FORMATTERS =
      ImmutableMap.<Integer, JsonFormatter>builder()
          .put(
              Types.INTEGER,
              (columnName, result, obj, i) -> obj.addProperty(columnName, result.getInt(i)))
          .put(
              Types.NUMERIC,
              (columnName, result, obj, i) -> obj.addProperty(columnName, result.getLong(i)))
          .put(
              Types.FLOAT,
              (columnName, result, obj, i) -> obj.addProperty(columnName, result.getFloat(i)))
          .put(
              Types.DOUBLE,
              (columnName, result, obj, i) -> obj.addProperty(columnName, result.getDouble(i)))
          .put(
              Types.BIGINT,
              (columnName, result, obj, i) -> obj.addProperty(columnName, result.getBigDecimal(i)))
          .put(
              Types.BINARY,
              (columnName, result, obj, i) ->
                  obj.addProperty(
                      columnName, Base64.getEncoder().encodeToString(result.getBytes(i))))
          .put(
              Types.BLOB,
              (columnName, result, obj, i) ->
                  obj.addProperty(
                      columnName, Base64.getEncoder().encodeToString(result.getBytes(i))))
          .put(
              Types.BOOLEAN,
              (columnName, result, obj, i) -> obj.addProperty(columnName, result.getBoolean(i)))
          .put(
              Types.VARCHAR,
              (columnName, result, obj, i) -> obj.addProperty(columnName, result.getString(i)))
          .put(
              Types.LONGNVARCHAR,
              (columnName, result, obj, i) -> obj.addProperty(columnName, result.getString(i)))
          .put(
              Types.DATE,
              (columnName, result, obj, i) ->
                  obj.addProperty(columnName, formatDate(result.getDate(i))))
          .build();

  @VisibleForTesting
  static String formatDate(Date date) {
    return DATE_FORMAT.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), UTC));
  }

  private static final JsonFormatter DEFAULT_FORMATTER =
      (columnName, result, obj, i) -> {
        var value = result.getObject(i);
        obj.addProperty(columnName, value == null ? null : value.toString());
      };

  @Override
  public String getKeyFromResult(ResultSet result) {
    return UUID.randomUUID().toString();
  }

  @Override
  public byte[] getValueBytes(ResultSet result, AttributeDescriptor<String> attributeDescriptor) {
    JsonObject obj = new JsonObject();
    try {
      ResultSetMetaData metadata = result.getMetaData();
      for (int i = 1; i <= metadata.getColumnCount(); i++) {
        int type = metadata.getColumnType(i);
        String columnName = metadata.getColumnName(i);
        JsonFormatter formatter = FORMATTERS.getOrDefault(type, DEFAULT_FORMATTER);
        formatter.addToObject(columnName, result, obj, i);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
    return obj.toString().getBytes();
  }
}
