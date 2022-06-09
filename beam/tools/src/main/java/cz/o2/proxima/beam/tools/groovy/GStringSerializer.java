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

import groovy.lang.GString;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Kryo;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.Serializer;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Input;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.io.Output;
import org.apache.beam.repackaged.kryo.com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer;
import org.codehaus.groovy.runtime.GStringImpl;

public class GStringSerializer extends Serializer<GString> {

  private final StringSerializer stringSerializer = new StringSerializer();

  @Override
  public void write(Kryo kryo, Output output, GString gString) {
    stringSerializer.write(kryo, output, gString.toString());
  }

  @Override
  public GString read(Kryo kryo, Input input, Class<GString> aClass) {
    String str = stringSerializer.read(kryo, input, String.class);
    return new GStringImpl(new Object[] {}, new String[] {str});
  }
}
