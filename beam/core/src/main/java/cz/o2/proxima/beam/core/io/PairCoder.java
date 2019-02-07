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
package cz.o2.proxima.beam.core.io;

import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

/**
 * A coder for {@link Pair}s.
 */
public class PairCoder<K, V> extends CustomCoder<Pair<K, V>> {

  public static <K, V> PairCoder<K, V> of(Coder<K> keyCoder, Coder<V> valueCoder) {
    return new PairCoder<>(keyCoder, valueCoder);
  }

  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;

  private PairCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public void encode(
      Pair<K, V> value, OutputStream outStream)
      throws CoderException, IOException {

    keyCoder.encode(value.getFirst(), outStream);
    valueCoder.encode(value.getSecond(), outStream);
  }

  @Override
  public Pair<K, V> decode(InputStream inStream)
      throws CoderException, IOException {

    return Pair.of(keyCoder.decode(inStream), valueCoder.decode(inStream));
  }

}
