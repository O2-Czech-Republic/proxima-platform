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
import cz.o2.proxima.core.repository.EntityDescriptor;
import cz.o2.proxima.core.scheme.ValueSerializer;
import cz.o2.proxima.direct.core.randomaccess.KeyValue;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public interface Converter<T> extends Serializable {
  default void setup() {}

  String getKeyFromResult(ResultSet result);

  default long getTimestampFromResult(ResultSet result) {
    return System.currentTimeMillis();
  }

  Object attributeValue(ResultSet resultSet, AttributeDescriptor<T> attr);

  /**
   * Convert single row of {@code result} to {@link KeyValue KeyValues}. The code must not call
   * {@link ResultSet#next()}.
   *
   * @param entityDescriptor entity descriprot
   * @param attributes the attributes to search the result for
   * @param stamp timestamp
   * @param result result to convert single line from
   * @return converted KeyValues.
   */
  default List<KeyValue<?>> asKeyValues(
      EntityDescriptor entityDescriptor,
      List<AttributeDescriptor<?>> attributes,
      long stamp,
      ResultSet result) {

    return attributes.stream()
        .map(
            a -> {
              String key = getKeyFromResult(result);
              return KeyValue.of(
                  entityDescriptor,
                  a,
                  key,
                  a.getName(),
                  new Offsets.Raw(key),
                  getValueBytes(result, a),
                  stamp);
            })
        .collect(Collectors.toList());
  }

  default byte[] getValueBytes(ResultSet result, AttributeDescriptor<?> attributeDescriptor) {
    @SuppressWarnings("unchecked")
    ValueSerializer<Object> serializer =
        (ValueSerializer<Object>) attributeDescriptor.getValueSerializer();
    try {
      Object column = result.getObject(attributeDescriptor.getName());
      return serializer.serialize(column);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}
