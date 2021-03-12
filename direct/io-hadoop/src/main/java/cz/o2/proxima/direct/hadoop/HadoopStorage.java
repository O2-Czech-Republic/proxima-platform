/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.o2.proxima.direct.core.DataAccessor;
import cz.o2.proxima.direct.core.DataAccessorFactory;
import cz.o2.proxima.direct.core.DirectDataOperator;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import java.net.URI;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/** Writer to HDFS. */
@Slf4j
@EqualsAndHashCode
public class HadoopStorage implements DataAccessorFactory {

  private static final long serialVersionUID = 1L;

  @Override
  public DataAccessor createAccessor(
      DirectDataOperator operator, AttributeFamilyDescriptor family) {
    return new HadoopDataAccessor(family.getEntity(), family.getStorageUri(), family.getCfg());
  }

  static URI remap(URI input) {
    if (input.getScheme().equals("hadoop")) {
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(input.getSchemeSpecificPart())
              && !("//" + input.getPath()).equals(input.getSchemeSpecificPart()),
          "When using generic `hadoop` scheme, please use scheme-specific part "
              + "for actual filesystem scheme (e.g. hadoop:file:///)");
      return URI.create(input.getSchemeSpecificPart());
    }
    return input;
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("hdfs") || uri.getScheme().equals("hadoop")
        ? Accept.ACCEPT
        : Accept.REJECT;
  }
}
