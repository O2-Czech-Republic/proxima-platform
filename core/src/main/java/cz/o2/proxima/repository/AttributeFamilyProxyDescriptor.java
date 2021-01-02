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

import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.StorageType;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Proxy attribute family applying transformations of attributes to and from private space to public
 * space.
 */
@Slf4j
public class AttributeFamilyProxyDescriptor extends AttributeFamilyDescriptor {

  private static final long serialVersionUID = 1L;

  static AttributeFamilyDescriptor of(
      Collection<AttributeProxyDescriptor<?>> attrs,
      AttributeFamilyDescriptor targetFamilyRead,
      AttributeFamilyDescriptor targetFamilyWrite) {

    return new AttributeFamilyProxyDescriptor(attrs, targetFamilyRead, targetFamilyWrite);
  }

  @Getter private final AttributeFamilyDescriptor targetFamilyRead;

  @Getter private final AttributeFamilyDescriptor targetFamilyWrite;

  @SuppressWarnings("unchecked")
  private AttributeFamilyProxyDescriptor(
      Collection<AttributeProxyDescriptor<?>> attrs,
      AttributeFamilyDescriptor targetFamilyRead,
      AttributeFamilyDescriptor targetFamilyWrite) {

    super(
        getFamilyName(targetFamilyRead, targetFamilyWrite),
        targetFamilyRead.getEntity(),
        targetFamilyWrite.getType() == targetFamilyRead.getType()
            ? targetFamilyRead.getType()
            : StorageType.REPLICA,
        getProxyUri(targetFamilyRead, targetFamilyWrite),
        unionMap(targetFamilyRead.getCfg(), targetFamilyWrite.getCfg()),
        (Collection) attrs,
        targetFamilyWrite.getType() == StorageType.PRIMARY
                && targetFamilyRead.getType() == StorageType.PRIMARY
            ? targetFamilyRead.getAccess()
            : AccessType.or(targetFamilyRead.getAccess(), AccessType.from("read-only")),
        targetFamilyRead.getFilter(),
        null);

    this.targetFamilyRead = targetFamilyRead;
    this.targetFamilyWrite = targetFamilyWrite;
  }

  private static String getFamilyName(
      AttributeFamilyDescriptor targetFamilyRead, AttributeFamilyDescriptor targetFamilyWrite) {

    return "proxy::" + targetFamilyRead.getName() + "::" + targetFamilyWrite.getName();
  }

  private static Map<String, Object> unionMap(Map<String, Object> left, Map<String, Object> right) {

    Map<String, Object> ret = new HashMap<>(left);
    ret.putAll(right);
    return ret;
  }

  @Override
  public boolean isProxy() {
    return true;
  }

  @Override
  public AttributeFamilyProxyDescriptor toProxy() {
    return this;
  }

  private static URI getProxyUri(
      AttributeFamilyDescriptor targetFamilyRead, AttributeFamilyDescriptor targetFamilyWrite) {

    try {
      return new URI(
          String.format("proxy://%s:%s", targetFamilyRead.getName(), targetFamilyWrite.getName())
              .replace("_", "-"));
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }
}
