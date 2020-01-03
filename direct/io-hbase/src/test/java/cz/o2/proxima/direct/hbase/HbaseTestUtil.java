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

import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/** Various test related utilities. */
class HbaseTestUtil {

  static byte[] bytes(String s) {
    return s.getBytes(Charset.forName("UTF-8"));
  }

  static void write(String key, String attribute, String value, long stamp, Table client)
      throws IOException {

    Put p = new Put(bytes(key));
    p.addColumn(bytes("u"), bytes(attribute), stamp, bytes(value));
    client.put(p);
  }

  private HbaseTestUtil() {}
}
