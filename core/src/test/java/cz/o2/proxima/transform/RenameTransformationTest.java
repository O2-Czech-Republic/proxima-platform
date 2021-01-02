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

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Test;

/** Test {@link RenameTransformation}. */
public class RenameTransformationTest {

  private final Repository repo =
      Repository.of(ConfigFactory.load("test-reference.conf").resolve());
  private final EntityDescriptor gateway = repo.getEntity("gateway");
  private final AttributeDescriptor<?> status = gateway.getAttribute("status");
  private final AttributeDescriptor<?> device = gateway.getAttribute("device.*");
  private final RenameTransformation transformation =
      new RenameTransformation(
          attr -> attr.equals(status) ? device : null,
          (name, tmp) -> device.toAttributePrefix() + name);
  private final long now = System.currentTimeMillis();

  @Test
  public void testRenameUpsert() {
    List<StreamElement> result = new ArrayList<>();
    int count =
        transformation.apply(
            StreamElement.upsert(
                gateway,
                status,
                UUID.randomUUID().toString(),
                "gw",
                status.getName(),
                now,
                new byte[] {1}),
            result::add);
    assertEquals(1, count);
    assertEquals(1, result.size());
    assertEquals(device.toAttributePrefix() + status.getName(), result.get(0).getAttribute());
    assertEquals(device, result.get(0).getAttributeDescriptor());
  }

  @Test
  public void testRenameDelete() {
    List<StreamElement> result = new ArrayList<>();
    int count =
        transformation.apply(
            StreamElement.delete(
                gateway, status, UUID.randomUUID().toString(), "gw", status.getName(), now),
            result::add);
    assertEquals(1, count);
    assertEquals(1, result.size());
    assertEquals(device.toAttributePrefix() + status.getName(), result.get(0).getAttribute());
    assertEquals(device, result.get(0).getAttributeDescriptor());
  }

  @Test
  public void testRenameTransformDeleteWildcard() {
    List<StreamElement> result = new ArrayList<>();
    transformation.setTransforms(
        attr -> attr.equals(device) ? device : null,
        (name, tmp) ->
            device.toAttributePrefix() + name.substring(tmp.toAttributePrefix().length()));

    int count =
        transformation.apply(
            StreamElement.deleteWildcard(gateway, device, UUID.randomUUID().toString(), "gw", now),
            result::add);
    assertEquals(1, count);
    assertEquals(1, result.size());
    assertEquals(device.getName(), result.get(0).getAttribute());
    assertEquals(device, result.get(0).getAttributeDescriptor());
  }

  @Test
  public void testInvalidSourceAttribute() {
    List<StreamElement> result = new ArrayList<>();
    int count =
        transformation.apply(
            StreamElement.delete(
                gateway,
                device,
                UUID.randomUUID().toString(),
                "gw",
                device.toAttributePrefix() + "1",
                now),
            result::add);
    assertEquals(0, count);
    assertTrue(result.isEmpty());
  }
}
