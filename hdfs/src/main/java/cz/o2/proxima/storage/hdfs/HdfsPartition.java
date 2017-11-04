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

import cz.o2.proxima.storage.batch.BoundedPartition;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

/**
 * {@code Partition} of files in HDFS.
 */
public class HdfsPartition extends BoundedPartition {

  @Getter
  private List<Path> files = new ArrayList<>();

  private long size = 0L;

  public HdfsPartition(int id) {
    super(id);
  }

  public void add(LocatedFileStatus file) {
    files.add(file.getPath());
    size += file.getLen();
  }

  @Override
  public long size() {
    return size;
  }



}
