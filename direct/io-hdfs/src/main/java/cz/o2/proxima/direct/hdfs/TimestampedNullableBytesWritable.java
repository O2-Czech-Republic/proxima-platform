/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** A {@code Writable} holding value of null. */
public class TimestampedNullableBytesWritable implements Writable {

  @Getter @Setter private long stamp;

  @Getter @Setter @Nullable private byte[] value = null;

  public TimestampedNullableBytesWritable() {}

  public TimestampedNullableBytesWritable(long stamp, @Nullable byte[] value) {
    this.stamp = stamp;
    this.value = value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(stamp);
    if (value == null) {
      WritableUtils.writeVInt(out, 0);
    } else {
      WritableUtils.writeVInt(out, 1);
      WritableUtils.writeCompressedByteArray(out, value);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.stamp = in.readLong();
    int hasValue = WritableUtils.readVInt(in);
    if (hasValue == 0) {
      value = null;
    } else {
      value = WritableUtils.readCompressedByteArray(in);
    }
  }

  @Override
  public String toString() {
    return "TimestampedNullableBytesWritable("
        + "stamp="
        + stamp
        + (value == null ? ",(null)" : (",value.length=" + value.length))
        + ")";
  }

  public boolean hasValue() {
    return value != null;
  }
}
