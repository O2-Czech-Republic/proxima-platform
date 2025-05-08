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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.checkerframework.checker.nullness.qual.Nullable;

@Value
public class LeftOrRight<L, R> {

  @ToString
  @EqualsAndHashCode(callSuper = false)
  static class LeftOrRightCoder<L, R> extends CustomCoder<LeftOrRight<L, R>> {
    private static final BooleanCoder boolCoder = BooleanCoder.of();
    private final Coder<L> leftCoder;
    private final Coder<R> rightCoder;

    LeftOrRightCoder(Coder<L> leftCoder, Coder<R> rightCoder) {
      this.leftCoder = leftCoder;
      this.rightCoder = rightCoder;
    }

    @Override
    public void encode(LeftOrRight<L, R> value, OutputStream outStream) throws IOException {
      if (value.isLeft()) {
        boolCoder.encode(true, outStream);
        leftCoder.encode(value.getLeft(), outStream);
      } else {
        boolCoder.encode(false, outStream);
        rightCoder.encode(value.getRight(), outStream);
      }
    }

    @Override
    public LeftOrRight<L, R> decode(InputStream inStream) throws IOException {
      boolean isLeft = boolCoder.decode(inStream);
      if (isLeft) {
        return new LeftOrRight<>(leftCoder.decode(inStream), null);
      }
      return new LeftOrRight<>(null, rightCoder.decode(inStream));
    }
  }

  public static <L, R> LeftOrRight<L, R> left(L left) {
    return new LeftOrRight<>(left, null);
  }

  public static <L, R> LeftOrRight<L, R> right(R right) {
    return new LeftOrRight<>(null, right);
  }

  @Nullable L left;
  @Nullable R right;

  boolean isLeft() {
    return left != null;
  }
}
