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
package cz.o2.proxima.transform;

import com.google.common.base.Preconditions;

/** Rename {@code _e.*} to {@code event.*} back and forth. */
public class EventRename implements ElementWiseProxyTransform {

  @Override
  public String fromProxy(String proxy) {
    Preconditions.checkArgument(proxy.startsWith("event."), "Invalid proxy attribute " + proxy);
    String suffix = proxy.substring(6);
    if (!suffix.equals("*")) {
      return "_e." + (Long.valueOf(suffix) + 1);
    }
    return "_e.*";
  }

  @Override
  public String toProxy(String raw) {
    Preconditions.checkArgument(raw.startsWith("_e."), "Invalid raw attribute " + raw);
    String suffix = raw.substring(3);
    if (!suffix.equals("*")) {
      return "event." + (Long.valueOf(suffix) - 1);
    }
    return "event.*";
  }
}
