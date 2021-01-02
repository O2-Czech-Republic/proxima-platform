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
package cz.o2.proxima.direct.cassandra;

import com.google.common.base.Strings;
import java.util.Date;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Represent a {@link java.util.Date} with {@code String} representation of epoch millis. */
@Slf4j
public class DateToLongConverter implements StringConverter<Date> {

  private static final long serialVersionUID = 1L;

  private static final Date MAX = new Date(Long.MAX_VALUE);
  private static final Date MIN = new Date(Long.MIN_VALUE);

  @Override
  public String asString(Date what) {
    return String.valueOf(what.getTime());
  }

  @Override
  public @Nullable Date fromString(String what) {
    if (Strings.isNullOrEmpty(what)) {
      return null;
    }
    try {
      return new Date(Long.parseLong(what));
    } catch (Exception ex) {
      log.error("Error converting {} to long", what, ex);
      return null;
    }
  }

  @Override
  public Date max() {
    return MAX;
  }

  @Override
  public Date min() {
    return MIN;
  }
}
