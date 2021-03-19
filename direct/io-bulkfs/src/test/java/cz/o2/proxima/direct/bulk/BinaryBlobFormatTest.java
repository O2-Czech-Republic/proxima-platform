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
package cz.o2.proxima.direct.bulk;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import cz.o2.proxima.gcloud.storage.proto.Serialization.Header;
import cz.o2.proxima.storage.StreamElement;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test suite for {@link BinaryBlobFormat} for reading and writing. */
@RunWith(Parameterized.class)
public class BinaryBlobFormatTest extends AbstractFileFormatTest {

  @Parameterized.Parameter public boolean gzip;

  public BinaryBlobFormatTest() throws URISyntaxException {}

  @Parameterized.Parameters
  public static Collection<Boolean> params() {
    return Arrays.asList(true, false);
  }

  @Override
  protected FileFormat getFileFormat() {
    return new BinaryBlobFormat(gzip);
  }

  @Test
  public void testReadInvalidMagic() throws IOException {
    byte[] bytes = Header.newBuilder().setMagic("INVALID").build().toByteArray();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(bytes.length);
    dos.write(bytes);
    dos.flush();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new BinaryBlobFormat.BinaryBlobReader(file, entity, bais);
        });
  }

  @Test
  public void testEmptyStream() throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(new byte[] {});
    List<StreamElement> elements =
        Lists.newArrayList(new BinaryBlobFormat.BinaryBlobReader(file, entity, bais).iterator());
    assertTrue(elements.isEmpty());
  }
}
