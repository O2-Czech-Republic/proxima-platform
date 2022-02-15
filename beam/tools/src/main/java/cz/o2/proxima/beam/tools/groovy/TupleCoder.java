/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.tools.groovy;

import groovy.lang.Tuple;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.util.VarInt;

/** Coder for {@link Tuple}s. */
public class TupleCoder extends Coder<Tuple<Object>> {

  private static final long serialVersionUID = 1L;

  public static TupleCoder of() {
    return new TupleCoder(KryoCoder.of());
  }

  public static TupleCoder of(KryoCoder<Object> coder) {
    return new TupleCoder(coder);
  }

  private final KryoCoder<Object> coder;
  private final int hashCode = System.identityHashCode(this);

  TupleCoder(KryoCoder<Object> coder) {
    this.coder = coder;
  }

  @Override
  public void encode(Tuple<Object> value, OutputStream outStream)
      throws CoderException, IOException {

    int size = value.size();
    VarInt.encode(size, outStream);
    for (int i = 0; i < size; i++) {
      coder.encode(value.get(i), outStream);
    }
  }

  @Override
  public Tuple<Object> decode(InputStream inStream) throws CoderException, IOException {

    int size = VarInt.decodeInt(inStream);
    Object[] read = new Object[size];
    for (int i = 0; i < size; i++) {
      read[i] = coder.decode(inStream);
    }
    return new Tuple<>(read);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // deterministic
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(coder);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof TupleCoder)) {
      return false;
    }
    return ((TupleCoder) obj).hashCode == hashCode;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
