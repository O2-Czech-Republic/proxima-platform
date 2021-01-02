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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** Test {@link ElementWiseProxyTransform}. */
public class ElementWiseProxyTransformTest {

  @Test
  public void testIdentity() {
    ProxyTransform transform = ElementWiseProxyTransform.identity();
    assertEquals("abc.def", transform.asElementWise().fromProxy("abc.def"));
    assertEquals("abc.def", transform.asElementWise().toProxy("abc.def"));
  }

  @Test
  public void testRenaming() {
    ProxyTransform transform = ElementWiseProxyTransform.renaming("proxy", "raw");
    assertEquals("raw.1", transform.asElementWise().fromProxy("proxy.1"));
    assertEquals("proxy.1", transform.asElementWise().toProxy("raw.1"));
  }

  @Test
  public void testDroppingUntilCharacter() {
    ProxyTransform transform = ElementWiseProxyTransform.droppingUntilCharacter('$', "myPrefix$");
    assertEquals("myPrefix$attr.1", transform.asElementWise().fromProxy("attr.1"));
    assertEquals("attr.1", transform.asElementWise().toProxy("myPrefix$attr.1"));
  }

  @Test
  public void testComposite() {
    ElementWiseProxyTransform transform =
        ElementWiseProxyTransform.composite(
            ElementWiseProxyTransform.droppingUntilCharacter('$', "prefix$"),
            ElementWiseProxyTransform.renaming("proxy", "raw"));
    assertEquals("proxy.1", transform.asElementWise().toProxy("prefix$raw.1"));
    assertEquals("prefix$raw.1", transform.asElementWise().fromProxy("proxy.1"));
  }
}
