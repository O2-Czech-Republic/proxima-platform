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
package cz.o2.proxima.repository;

import lombok.Getter;

/**
 * Proxy to another attribute.
 */
class AttributeProxyDescriptorImpl<T> extends AttributeDescriptorBase<T> {

  @Getter
  private final AttributeDescriptor<T> readTarget;

  @Getter
  private final ProxyTransform readTransform;

  @Getter
  private final AttributeDescriptor<T> writeTarget;

  @Getter
  private final ProxyTransform writeTransform;

  AttributeProxyDescriptorImpl(
      String name,
      AttributeDescriptor<T> readTarget,
      ProxyTransform readTransform,
      AttributeDescriptor<T> writeTarget,
      ProxyTransform writeTransform) {

    super(name, readTarget, writeTarget);
    this.readTarget = readTarget;
    this.readTransform = readTransform;
    this.writeTarget = writeTarget;
    this.writeTransform = writeTransform;
  }

  @Override
  public String toString() {
    return "AttributeProxyDescriptorImpl("
        + "readTarget=" + readTarget
        + ", writeTarget=" + writeTarget
        + ", name=" + name
        + ")";
  }

}
