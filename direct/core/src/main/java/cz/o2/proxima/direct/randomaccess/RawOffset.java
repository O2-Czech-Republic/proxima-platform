/**
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
package cz.o2.proxima.direct.randomaccess;

import cz.o2.proxima.annotations.Stable;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Offset that is represented by raw string. */
@Stable
@EqualsAndHashCode
public class RawOffset implements RandomOffset {

  private static final long serialVersionUID = 1L;

  @Getter private final String offset;

  public RawOffset(String offset) {
    this.offset = offset;
  }

  @Override
  public String toString() {
    return "RawOffset(" + offset + ")";
  }
}
