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
package cz.o2.proxima.storage;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import org.junit.Test;

/** Tests for {@link UriUtil}. */
public class UriUtilTest {

  @Test
  public void testParseQueryWithSingleParam() throws URISyntaxException {
    URI uri = new URI("proxima://foo/bar?isItCool=yes");
    Map<String, String> params = UriUtil.parseQuery(uri);
    assertEquals("yes", params.get("isItCool"));
  }

  @Test
  public void testParseQueryWithoutParams() throws URISyntaxException {
    URI uri = new URI("proxima://foo/bar");
    assertEquals(0, UriUtil.parseQuery(uri).size());
  }

  @Test
  public void testParseQueryWithMultipleParams() throws URISyntaxException {
    URI uri = new URI("proxima://foo/bar?isItCool=yes&param=foo&foo=bar");
    Map<String, String> params = UriUtil.parseQuery(uri);
    assertEquals(3, params.size());
    assertEquals("yes", params.get("isItCool"));
    assertEquals("foo", params.get("param"));
    assertEquals("bar", params.get("foo"));
  }

  @Test
  public void testEncodedQueryParams() throws URISyntaxException {
    URI uri = new URI("proxima://foo/bar?encoded=proxima%2Fparam%3Ffoo%26bar");
    Map<String, String> params = UriUtil.parseQuery(uri);
    assertEquals(1, params.size());
    assertEquals("proxima/param?foo&bar", params.get("encoded"));
  }

  @Test
  public void testGetPathNormalizer() throws URISyntaxException {
    URI uri = new URI("proxima://foo/bar/");
    assertEquals("bar", UriUtil.getPathNormalized(uri));
    uri = new URI("https://example.com/foo/bar/");
    assertEquals("foo/bar", UriUtil.getPathNormalized(uri));
  }

  @Test
  public void testGetPathNormalizerWithEncodedPath() throws URISyntaxException {
    URI uri = new URI("https://example.com/foo%2Fbar/beer");
    assertEquals("foo/bar/beer", UriUtil.getPathNormalized(uri));
  }

  @Test
  public void testParsePath() throws URISyntaxException {
    URI uri = new URI("proxima://foo/bring/me/some/drink");
    assertEquals(Arrays.asList("bring", "me", "some", "drink"), UriUtil.parsePath(uri));
    uri = new URI("proxima://host/encoded%2Fis%2Ffine/lets/drink");
    assertEquals(Arrays.asList("encoded/is/fine", "lets", "drink"), UriUtil.parsePath(uri));
  }
}
