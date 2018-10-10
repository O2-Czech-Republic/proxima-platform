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
package cz.o2.proxima.source;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.commitlog.Position;
import org.junit.Test;

/**
 * Test suite for {@link BoundedStreamSource}.
 */
public class BoundedStreamSourceTest extends BoundedSourceTest {

  @Test(timeout = 10000)
  public void testSimpleConsume() throws InterruptedException {
    testSimpleConsume(getWriter(attr), createSource(attr));
  }

  @Test(timeout = 10000)
  public void testSimpleConsumeWildcard() throws InterruptedException {
    testSimpleConsumeWildcard(getWriter(wildcard), createSource(wildcard));
  }

  private BoundedStreamSource createSource(AttributeDescriptor<byte[]> desc) {
    return createSource(desc, Position.OLDEST);
  }

  @SuppressWarnings("unchecked")
  private BoundedStreamSource createSource(
      AttributeDescriptor<byte[]> desc, Position position) {

    return repo.getFamiliesForAttribute(desc)
        .stream()
        .filter(af -> af.getAccess().canReadCommitLog())
        .findAny()
        .flatMap(af -> af.getCommitLogReader())
        .map(reader -> BoundedStreamSource.of(reader, position))
        .orElseThrow(() -> new IllegalArgumentException(
            "Attribute " + desc + " has no commit-log reader"));
  }

  @Override
  EntityDescriptor getEntity(Repository repo) {
    return repo
        .findEntity("dummy")
        .orElseThrow(() -> new IllegalStateException("Missing entity dummy"));
  }

  @SuppressWarnings("unchecked")
  @Override
  AttributeDescriptor<byte[]> getAttr(EntityDescriptor entity) {
    return (AttributeDescriptor) entity
      .findAttribute("data")
      .orElseThrow(() -> new IllegalStateException("Missing attribute data"));
  }

  @SuppressWarnings("unchecked")
  @Override
  AttributeDescriptor<byte[]> getWildcard(EntityDescriptor entity) {
    return (AttributeDescriptor) entity
        .findAttribute("wildcard.*")
        .orElseThrow(() -> new IllegalStateException("Missing attribute wildcard.*"));
  }

}
