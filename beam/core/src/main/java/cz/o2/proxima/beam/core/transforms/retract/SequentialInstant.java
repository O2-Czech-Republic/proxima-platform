/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core.transform.retract;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.joda.time.Instant;

@Value
public class SequentialInstant implements Comparable<SequentialInstant> {

  @EqualsAndHashCode(callSuper = false)
  public static class Coder extends CustomCoder<SequentialInstant> {
    private static InstantCoder instantCoder = InstantCoder.of();
    private static VarLongCoder longCoder = VarLongCoder.of();

    @Override
    public void encode(SequentialInstant value, OutputStream outStream) throws IOException {
      instantCoder.encode(value.getTimestamp(), outStream);
      longCoder.encode(value.getSeqId(), outStream);
    }

    @Override
    public SequentialInstant decode(InputStream inStream) throws IOException {
      return new SequentialInstant(instantCoder.decode(inStream), longCoder.decode(inStream));
    }
  }

  Instant timestamp;
  long seqId;

  @Override
  public int compareTo(SequentialInstant other) {
    int cmp = this.timestamp.compareTo(other.getTimestamp());
    if (cmp == 0) {
      return Long.compare(seqId, other.getSeqId());
    }
    return cmp;
  }
}
