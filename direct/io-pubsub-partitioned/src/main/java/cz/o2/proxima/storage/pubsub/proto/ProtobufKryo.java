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
package cz.o2.proxima.storage.pubsub.proto;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.GeneratedMessage;
import cz.seznam.euphoria.core.annotation.stability.Experimental;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A {@link Serializer} for {@link Kryo} for {@link GeneratedMessage}.
 */
@Experimental
public class ProtobufKryo extends Serializer<Object> {

  private final Method parser;
  private final Method serializer;

  @SuppressWarnings("unchecked")
  public ProtobufKryo(Class theClass) {
    try {
      parser = theClass.getDeclaredMethod("parseFrom", InputStream.class);
      parser.setAccessible(true);
      serializer = theClass.getDeclaredMethod("writeTo", theClass, OutputStream.class);
      serializer.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          theClass.toString() + " is not compatible with " + getClass().getName(), e);
    }
  }

  @Override
  public void write(Kryo kryo, Output output, Object message) {
    try {
      serializer.invoke(message, output);
    } catch (Exception e) {
      // This isn't supposed to happen with a Kryo output.
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object read(Kryo kryo, Input input, Class gmClass) {
    try {
      return parser.invoke(null, input);
    } catch (InvocationTargetException | IllegalAccessException e) {
      // These really shouldn't happen
      throw new IllegalArgumentException(e);
    }
  }
}
