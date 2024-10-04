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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import lombok.Value;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

@Value
public class StateValue {
  public static class StateValueCoder extends CustomCoder<StateValue> {
    private static final StateValueCoder INSTANCE = new StateValueCoder();
    private static final ByteArrayCoder BAC = ByteArrayCoder.of();
    private static final StringUtf8Coder SUC = StringUtf8Coder.of();

    private StateValueCoder() {}

    @Override
    public void encode(StateValue value, OutputStream outStream) throws IOException {
      BAC.encode(value.getKey(), outStream);
      SUC.encode(value.getName(), outStream);
      BAC.encode(value.getValue(), outStream);
    }

    @Override
    public StateValue decode(InputStream inStream) throws IOException {
      return new StateValue(BAC.decode(inStream), SUC.decode(inStream), BAC.decode(inStream));
    }
  }

  public static StateValueCoder coder() {
    return StateValueCoder.INSTANCE;
  }

  byte[] key;
  String name;
  byte[] value;
}
