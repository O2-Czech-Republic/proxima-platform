/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ProxyTransform;

public class AsymmetricWrite implements ProxyTransform {

  private String target;

  @Override
  public void setup(AttributeDescriptor<?> target) {
    this.target = target.toAttributePrefix();
  }

  @Override
  public String fromProxy(String proxy) {
    int pos = proxy.indexOf('.');
    return "_e." + proxy.substring(pos + 1);
  }

  @Override
  public String toProxy(String raw) {
    int pos = raw.indexOf('.');
    return target + raw.substring(pos + 1);
  }

}
