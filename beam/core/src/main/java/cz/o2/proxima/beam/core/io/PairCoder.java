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
package cz.o2.proxima.beam.core.io;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import lombok.Getter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

/** A coder for {@link Pair}s. */
@Internal
public class PairCoder<K, V> extends CustomCoder<Pair<K, V>> {

  private static final long serialVersionUID = 1L;

  public static <K, V> PairCoder<K, V> of(Coder<K> keyCoder, Coder<V> valueCoder) {
    return new PairCoder<>(keyCoder, valueCoder);
  }

  public static <K, V> TypeDescriptor<Pair<K, V>> descriptor(
      TypeDescriptor<K> key, TypeDescriptor<V> value) {

    return new TypeDescriptor<Pair<K, V>>() {}.where(new TypeParameter<K>() {}, key)
        .where(new TypeParameter<V>() {}, value);
  }

  @Getter private final Coder<K> keyCoder;
  @Getter private final Coder<V> valueCoder;

  private PairCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public void encode(Pair<K, V> value, OutputStream outStream) throws IOException {

    keyCoder.encode(value.getFirst(), outStream);
    valueCoder.encode(value.getSecond(), outStream);
  }

  @Override
  public Pair<K, V> decode(InputStream inStream) throws IOException {
    return Pair.of(keyCoder.decode(inStream), valueCoder.decode(inStream));
  }

  @Override
  public TypeDescriptor<Pair<K, V>> getEncodedTypeDescriptor() {
    return descriptor(keyCoder.getEncodedTypeDescriptor(), valueCoder.getEncodedTypeDescriptor());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof PairCoder)) {
      return false;
    }
    PairCoder<?, ?> other = (PairCoder<?, ?>) obj;
    return other.keyCoder.equals(keyCoder) && other.valueCoder.equals(valueCoder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyCoder, valueCoder);
  }

  @Override
  public void verifyDeterministic() {}
}
