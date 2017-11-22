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

package cz.o2.proxima.storage.hdfs;

import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.StorageDescriptor;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer to HDFS.
 */
public class HdfsStorage extends StorageDescriptor {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsStorage.class);

  public HdfsStorage() {
    super(Arrays.asList("hdfs"));
  }

  @Override
  public DataAccessor getAccessor(
      EntityDescriptor entityDesc,
      URI uri, Map<String, Object> cfg) {

    try {
      return new HdfsDataAccessor(entityDesc, uri, cfg);
    } catch (IOException ex) {
      LOG.error(
          "Failed to instantiate {}", HdfsDataAccessor.class.getName(), ex);
      throw new RuntimeException(ex);
    }
  }

}
