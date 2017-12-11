/**
 * Copyright 2017 O2 Czech Republic, a.s.
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
package cz.o2.proxima.repository;

import lombok.Getter;

/**
 * Proxy to another attribute.
 */
class AttributeProxyDescriptorImpl<T>
    extends AttributeDescriptorBase<T> {

  @Getter
  private final AttributeDescriptorBase<T> target;

  @Getter
  private final ProxyTransform transform;

  AttributeProxyDescriptorImpl(
      String name,
      AttributeDescriptorBase<T> target,
      ProxyTransform transform) {

    super(name, target);
    this.target = target;
    this.transform = transform;
  }

  @Override
  public String toString() {
    return "AttributeProxyDescriptorImpl("
        + "target=" + target
        + ", name=" + name
        + ")";
  }

}
