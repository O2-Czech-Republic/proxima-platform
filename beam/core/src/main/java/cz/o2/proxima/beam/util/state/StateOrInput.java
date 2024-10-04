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
package cz.o2.proxima.beam.util.state;

import cz.o2.proxima.beam.util.state.StateValue.StateValueCoder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import lombok.Value;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.checkerframework.checker.nullness.qual.Nullable;

@Value
public class StateOrInput<T> {

  public static class StateOrInputCoder<T> extends CustomCoder<StateOrInput<T>> {

    private final ByteCoder byteCoder = ByteCoder.of();
    private final StateValueCoder stateCoder = StateValue.coder();
    private final Coder<T> inputCoder;

    private StateOrInputCoder(Coder<T> inputCoder) {
      this.inputCoder = inputCoder;
    }

    @Override
    public void encode(StateOrInput<T> value, OutputStream outStream) throws IOException {
      byteCoder.encode(value.getTag(), outStream);
      if (value.isState()) {
        stateCoder.encode(value.getState(), outStream);
      } else {
        inputCoder.encode(value.getInput(), outStream);
      }
    }

    @Override
    public StateOrInput<T> decode(InputStream inStream) throws IOException {
      byte tag = byteCoder.decode(inStream);
      if (tag == 0) {
        return new StateOrInput<>(tag, stateCoder.decode(inStream), null);
      }
      return new StateOrInput<>(tag, null, inputCoder.decode(inStream));
    }

    @Override
    public int hashCode() {
      return Objects.hash(byteCoder, stateCoder, inputCoder);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof StateOrInputCoder)) {
        return false;
      }
      StateOrInputCoder<?> other = (StateOrInputCoder<?>) obj;
      return other.inputCoder.equals(this.inputCoder);
    }
  }

  public static <T> StateOrInputCoder<T> coder(Coder<T> inputCoder) {
    return new StateOrInputCoder<>(inputCoder);
  }

  public static <T> StateOrInput<T> state(StateValue state) {
    return new StateOrInput<>((byte) 0, state, null);
  }

  public static <T> StateOrInput<T> input(T input) {
    return new StateOrInput<>((byte) 1, null, input);
  }

  byte tag;
  @Nullable StateValue state;
  @Nullable T input;

  boolean isState() {
    return tag == 0;
  }
}
