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
package cz.o2.proxima.direct.io.hadoop;

import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.direct.core.batch.BoundedPartition;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/** {@code Partition} of files in HDFS. */
public class HadoopPartition extends BoundedPartition {

  private static final long serialVersionUID = 1L;

  @Getter private final List<HadoopPath> paths = new ArrayList<>();

  private long size = 0L;

  public HadoopPartition(int id) {
    super(id);
  }

  public void add(HadoopPath path) {
    paths.add(path);
    size += ExceptionUtils.uncheckedFactory(() -> path.getFileStatus().getLen());
  }

  @Override
  public long size() {
    return size;
  }
}
