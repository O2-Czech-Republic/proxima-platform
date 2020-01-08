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
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface Converter<T> extends Serializable {
  default void setup() {}

  String getKeyFromResult(ResultSet result) throws SQLException;

  default long getTimestampFromResult(ResultSet result) throws SQLException {
    return System.currentTimeMillis();
  }

  Object attributeValue(ResultSet resultSet, AttributeDescriptor<T> attr) throws SQLException;
}
