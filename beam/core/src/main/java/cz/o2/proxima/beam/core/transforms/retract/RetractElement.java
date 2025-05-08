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
package cz.o2.proxima.beam.core.transforms.retract;

import cz.o2.proxima.core.functional.UnaryFunction;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

@ToString
@EqualsAndHashCode
@Getter
public class RetractElement<T> {

  @EqualsAndHashCode(callSuper = false)
  public static class Coder<T> extends CustomCoder<RetractElement<T>> {

    public static <T> Coder<T> of(org.apache.beam.sdk.coders.Coder<T> valueCoder) {
      return new Coder<>(valueCoder);
    }

    private static final VarLongCoder longCoder = VarLongCoder.of();
    private static final BooleanCoder boolCoder = BooleanCoder.of();

    @Getter private final org.apache.beam.sdk.coders.Coder<T> valueCoder;

    private Coder(org.apache.beam.sdk.coders.Coder<T> valueCoder) {
      this.valueCoder = valueCoder;
    }

    @Override
    public void encode(RetractElement<T> value, OutputStream outStream) throws IOException {
      valueCoder.encode(value.getValue(), outStream);
      longCoder.encode(value.getSeqId(), outStream);
      boolCoder.encode(value.isAddition(), outStream);
    }

    @Override
    public RetractElement<T> decode(InputStream inStream) throws IOException {
      return new RetractElement<>(
          valueCoder.decode(inStream), longCoder.decode(inStream), boolCoder.decode(inStream));
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getCoderArguments() {
      return List.of(valueCoder);
    }

    @Override
    public TypeDescriptor<RetractElement<T>> getEncodedTypeDescriptor() {
      return new TypeDescriptor<RetractElement<T>>() {}.where(
          new TypeParameter<T>() {}, valueCoder.getEncodedTypeDescriptor());
    }
  }

  public static <T> RetractElement<T> ofAddition(T value, long seqId) {
    return new RetractElement<>(value, seqId, true);
  }

  public static <T> RetractElement<T> ofDeletion(T value, long seqId) {
    return new RetractElement<>(value, seqId, false);
  }

  private final T value;
  private final long seqId;
  private final boolean isAddition;

  public RetractElement(T value, long seqId, boolean isAddition) {
    this.value = value;
    this.seqId = seqId;
    this.isAddition = isAddition;
  }

  public <M> RetractElement<M> mapped(UnaryFunction<T, M> map) {
    return isAddition() ? ofAddition(map.apply(value), seqId) : ofDeletion(map.apply(value), seqId);
  }
}
