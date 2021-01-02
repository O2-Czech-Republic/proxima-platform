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
package cz.o2.proxima.repository;

import com.google.common.base.MoreObjects;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.transform.ProxyTransform;
import java.net.URI;
import javax.annotation.Nullable;
import lombok.Getter;

/** Proxy to another attribute. */
public class AttributeProxyDescriptor<T> extends AttributeDescriptorBase<T> {

  private static final long serialVersionUID = 1L;

  @Getter private final AttributeDescriptor<T> readTarget;

  @Nullable @Getter private final ProxyTransform readTransform;

  @Getter private final AttributeDescriptor<T> writeTarget;

  @Nullable @Getter private final ProxyTransform writeTransform;

  AttributeProxyDescriptor(
      String name,
      AttributeDescriptor<T> readTarget,
      ProxyTransform readTransform,
      AttributeDescriptor<T> writeTarget,
      ProxyTransform writeTransform,
      boolean replica,
      URI schemeURI,
      ValueSerializer<T> valueSerializer) {

    super(name, readTarget, writeTarget, replica, schemeURI, valueSerializer);
    this.readTarget = readTarget;
    this.readTransform = readTransform;
    this.writeTarget = writeTarget;
    this.writeTransform = writeTransform;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("readTarget", readTarget)
        .add("writeTarget", writeTarget)
        .add("name", name)
        .toString();
  }

  @Override
  public boolean isProxy() {
    return true;
  }

  public boolean isAsymmetric() {
    return readTarget != writeTarget || readTransform != writeTransform;
  }
}
