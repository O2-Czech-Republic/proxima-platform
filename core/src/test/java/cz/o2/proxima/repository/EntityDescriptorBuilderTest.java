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

import com.typesafe.config.ConfigFactory;
import java.net.URI;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class EntityDescriptorBuilderTest {
  final Repository repository = Repository.of(ConfigFactory.defaultApplication().resolve());

  @Parameters({"attr.*,attr", "attr,attr.*"})
  @Test(expected = IllegalArgumentException.class)
  public void testBuildEntityWithDuplicateAttributeName(String first, String second) {
    EntityDescriptor.newBuilder()
        .addAttribute(createSimpleAttributeWithName(first))
        .addAttribute(createSimpleAttributeWithName(second));
  }

  private AttributeDescriptor<byte[]> createSimpleAttributeWithName(String name) {
    return AttributeDescriptor.newBuilder(repository)
        .setEntity("e")
        .setName(name)
        .setSchemeUri(URI.create("bytes:///"))
        .build();
  }
}
