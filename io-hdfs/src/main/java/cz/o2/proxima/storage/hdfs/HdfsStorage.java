/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.storage.hdfs;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.StorageDescriptor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;

/**
 * Writer to HDFS.
 */
@Slf4j
public class HdfsStorage extends StorageDescriptor {

  public HdfsStorage() {
    super(Arrays.asList("hdfs", "hadoop"));
  }

  @Override
  public DataAccessor getAccessor(EntityDescriptor entityDesc,
                                  URI uri, Map<String, Object> cfg) {

    return new HdfsDataAccessor(entityDesc, remap(uri), cfg);
  }

  private static URI remap(URI input) {
    if (input.getScheme().equals("hadoop")) {
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(input.getSchemeSpecificPart()),
          "When using generic `hadoop` scheme, please use scheme-specific part "
              + "for actual filesystem scheme");
      try {
        return new URI(input.toString().replace(
            "hadoop:" + input.getSchemeSpecificPart(), input.getSchemeSpecificPart()));
      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex);
      }
    }
    return input;
  }

}
