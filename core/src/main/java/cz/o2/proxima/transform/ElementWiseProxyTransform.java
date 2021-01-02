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

import cz.o2.proxima.annotations.Evolving;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.AttributeDescriptor;

@Evolving
public interface ElementWiseProxyTransform extends ProxyTransform, DataOperatorAgnostic {

  static ElementWiseProxyTransform identity() {
    return new ElementWiseProxyTransform() {
      @Override
      public String fromProxy(String proxy) {
        return proxy;
      }

      @Override
      public String toProxy(String raw) {
        return raw;
      }
    };
  }

  static ElementWiseProxyTransform composite(ElementWiseProxyTransform... transforms) {

    return new ElementWiseProxyTransform() {
      @Override
      public String fromProxy(String proxy) {
        String tmp = proxy;
        for (int i = transforms.length - 1; i >= 0; i--) {
          tmp = transforms[i].fromProxy(tmp);
        }
        return tmp;
      }

      @Override
      public String toProxy(String raw) {
        String tmp = raw;
        for (ElementWiseProxyTransform t : transforms) {
          tmp = t.toProxy(tmp);
        }
        return tmp;
      }
    };
  }

  /**
   * Proxy renaming attribute.
   *
   * @param proxy name of proxy attribute
   * @param raw name of raw attribute
   * @return the transform performing the rename operation
   */
  static ElementWiseProxyTransform renaming(String proxy, String raw) {
    return new ElementWiseProxyTransform() {

      @Override
      public String fromProxy(String s) {
        if (s.startsWith(proxy)) {
          return raw + s.substring(proxy.length());
        }
        return s;
      }

      @Override
      public String toProxy(String s) {
        if (s.startsWith(raw)) {
          return proxy + s.substring(raw.length());
        }
        return s;
      }
    };
  }

  static ElementWiseProxyTransform droppingUntilCharacter(char character, String rawPrefix) {
    return new ElementWiseProxyTransform() {

      @Override
      public String fromProxy(String proxy) {
        return rawPrefix + proxy;
      }

      @Override
      public String toProxy(String raw) {
        int pos = raw.indexOf(character);
        if (pos > 0) {
          return raw.substring(pos + 1);
        }
        return raw;
      }
    };
  }

  static ElementWiseProxyTransform from(
      UnaryFunction<String, String> fromProxy, UnaryFunction<String, String> toProxy) {

    return new ElementWiseProxyTransform() {
      @Override
      public String fromProxy(String proxy) {
        return fromProxy.apply(proxy);
      }

      @Override
      public String toProxy(String raw) {
        return toProxy.apply(raw);
      }
    };
  }

  interface ProxySetupContext {

    /**
     * Retrieve attribute that is the proxied attribute.
     *
     * @return descriptor of proxied attribute
     */
    AttributeDescriptor<?> getProxyAttribute();

    /**
     * Retrieve attribute that is target of this transform (attribute to read from, or to write to).
     *
     * @return decriptor of target attribute
     */
    AttributeDescriptor<?> getTargetAttribute();

    /** @return {@code true} is this is read transform */
    boolean isReadTransform();

    /** @return {@code true} is this is read transform */
    boolean isWriteTransform();

    default boolean isSymmetric() {
      return isReadTransform() && isWriteTransform();
    }
  }

  /**
   * Apply transformation to attribute name from proxy naming.
   *
   * @param proxy name of the attribute in proxy namespace
   * @return the raw attribute
   */
  String fromProxy(String proxy);

  /**
   * Apply transformation to attribute name to proxy naming.
   *
   * @param raw the raw attribute name
   * @return the proxy attribute name
   */
  String toProxy(String raw);

  /**
   * Setup this transform for given target attribute.
   *
   * @param target the target attribute descriptor
   * @deprecated use {@link #setup(ProxySetupContext)}
   */
  @Deprecated
  default void setup(AttributeDescriptor<?> target) {
    // nop
  }

  /**
   * Setup this transform with specified target and source attribute.
   *
   * @param context context of the setup
   */
  default void setup(ProxySetupContext context) {
    setup(context.getTargetAttribute());
  }
}
