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
package cz.o2.proxima.direct.hadoop;

import cz.o2.proxima.direct.bulk.AbstractBulkFileSystemAttributeWriter;
import cz.o2.proxima.direct.core.BulkAttributeWriter;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.util.ExceptionUtils;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;

/** Bulk attribute writer to Hadoop FileSystem. */
@Slf4j
public class HadoopBulkAttributeWriter extends AbstractBulkFileSystemAttributeWriter {

  private final HadoopFileSystem targetFs;
  private final HadoopDataAccessor accessor;

  public HadoopBulkAttributeWriter(HadoopDataAccessor accessor, Context context) {
    super(
        accessor.getEntityDesc(),
        accessor.getUriRemapped(),
        accessor.getTemporaryHadoopFs(),
        accessor.getTemporaryNamingConvention(),
        accessor.getFormat(),
        context,
        accessor.getRollInterval(),
        accessor.getAllowedLateness());

    this.targetFs = accessor.getHadoopFs();
    this.accessor = accessor;
  }

  @Override
  public URI getUri() {
    return accessor.getUri();
  }

  @Override
  protected void flush(Bulk bulk) {
    HadoopPath path = (HadoopPath) bulk.getPath();
    ExceptionUtils.unchecked(
        () -> path.move((HadoopPath) targetFs.newPath(bulk.getMaxTs() - getRollPeriodMs())));
    log.info("Flushed bulk {}", bulk);
  }

  @Override
  public BulkAttributeWriter.Factory<?> asFactory() {
    final HadoopDataAccessor accessor = this.accessor;
    final Context context = getContext();
    return repo -> new HadoopBulkAttributeWriter(accessor, context);
  }
}
