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
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Kryo;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Serializer;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Input;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Output;

/** Kryo serializer for {@link Tuple}s. */
public class TupleSerializer extends Serializer<Tuple<Object>> {

  @Override
  public void write(Kryo kryo, Output output, Tuple<Object> t) {
    output.writeInt(t.size());
    for (int i = 0; i < t.size(); i++) {
      kryo.writeClassAndObject(output, t.get(i));
    }
  }

  @Override
  public Tuple<Object> read(Kryo kryo, Input input, Class<Tuple<Object>> type) {
    int size = input.readInt();
    Object[] objects = new Object[size];
    for (int i = 0; i < size; i++) {
      objects[i] = kryo.readClassAndObject(input);
    }
    return new Tuple<>(objects);
  }
}
