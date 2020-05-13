/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
package cz.o2.proxima.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** Utilities related to string compression. */
public class StringCompressions {

  private static final int BUFFER_SIZE = 4096;

  private StringCompressions() {
    // No-op.
  }

  /**
   * Gzip compress a given string.
   *
   * @param toCompress String to gzip.
   * @param encoding Input string encoding.
   * @return Gzipped byte representation.
   */
  public static byte[] gzip(String toCompress, Charset encoding) {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final GZIPOutputStream gos = new GZIPOutputStream(baos)) {
      gos.write(toCompress.getBytes(encoding));
      gos.finish();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to zip content.", e);
    }
  }

  /**
   * Un-gzip byte buffer to string.
   *
   * @param compressed Compressed buffer.
   * @param encoding Output string encoding.
   * @return String.
   */
  public static String gunzip(byte[] compressed, Charset encoding) {
    try (final ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
        final GZIPInputStream gis = new GZIPInputStream(bais);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final byte[] buffer = new byte[BUFFER_SIZE];
      int numRead;
      while ((numRead = gis.read(buffer, 0, buffer.length)) != -1) {
        baos.write(buffer, 0, numRead);
      }
      return new String(baos.toByteArray(), encoding);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to unzip content.", e);
    }
  }
}
