/*
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.flink.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import cz.o2.proxima.time.Watermarks;
import cz.o2.proxima.util.Pair;
import java.io.ByteArrayInputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class FlinkGlobalWatermarkTrackerTest {

  @Test
  public void testInitWithoutVertices() {
    Map<String, String> urlToContent =
        ImmutableMap.of(
            "http://dummy/jobs",
            createJobsPayload(),
            "http://dummy/jobs/dummy-job-id",
            createJobPayload(
                IntStream.range(0, 5)
                    .mapToObj(i -> UUID.randomUUID().toString())
                    .collect(Collectors.toList())));
    FlinkGlobalWatermarkTracker tracker =
        newTracker(fromMap(urlToContent, defaultWatermarkPayload()));

    assertEquals(Watermarks.MAX_WATERMARK, tracker.getWatermark());
  }

  @Test
  public void testGetWithoutVertices() {
    Map<String, String> urlToContent =
        ImmutableMap.of(
            "http://dummy/jobs",
            createJobsPayload(),
            "http://dummy/jobs/dummy-job-id",
            createJobPayload(
                IntStream.range(0, 5)
                    .mapToObj(i -> UUID.randomUUID().toString())
                    .collect(Collectors.toList())));
    FlinkGlobalWatermarkTracker tracker =
        newTracker(fromMap(urlToContent, defaultWatermarkPayload()));
    assertEquals(Watermarks.MAX_WATERMARK, tracker.getWatermark());
  }

  @Test
  public void testGetWithVertices() {
    List<String> names =
        IntStream.range(0, 5)
            .mapToObj(i -> UUID.randomUUID().toString())
            .collect(Collectors.toList());
    Map<String, String> urlToContent =
        ImmutableMap.of(
            "http://dummy/jobs",
            createJobsPayload(),
            "http://dummy/jobs/dummy-job-id",
            createJobPayload(names, name -> name));
    long now = 1234567890000L;
    FlinkGlobalWatermarkTracker tracker =
        newTracker(
            fromMap(
                urlToContent,
                watermarksForIds(
                    names
                        .stream()
                        .map(n -> Pair.of(n, now + 5 + names.indexOf(n)))
                        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)))));
    assertEquals(1234567890005L, tracker.getWatermark());
  }

  @Test
  public void testGetApiUrl() throws MalformedURLException {
    URL apiUrl =
        FlinkGlobalWatermarkTracker.getApiUrlFor(new URL("http://localhost:1234"), "api/v1/test");
    assertEquals("http://localhost:1234/api/v1/test", apiUrl.toString());
  }

  @Test
  public void testJsonParse() {
    String payload = createJobsPayload();
    JsonElement parsed =
        FlinkGlobalWatermarkTracker.parseInputStreamToJson(
            new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)));
    assertNotNull(parsed);
  }

  private String createJobsPayload() {
    JsonObject obj = new JsonObject();
    JsonArray jobs = new JsonArray();
    JsonObject job = new JsonObject();
    job.addProperty("name", "dummy-job");
    job.addProperty("id", "dummy-job-id");
    jobs.add(job);
    obj.add("jobs", jobs);
    return obj.toString();
  }

  private Function<URL, String> watermarksForIds(Map<String, Long> watermarks) {
    return url -> {
      String[] parts = url.toString().split("/");
      String vertex = parts[parts.length - 2];
      return getWatermarkJson(watermarks.get(vertex));
    };
  }

  private static Function<URL, String> defaultWatermarkPayload() {
    return url -> getWatermarkJson(Watermarks.MAX_WATERMARK);
  }

  private static String getWatermarkJson(long stamp) {
    JsonArray watermarks = new JsonArray();
    JsonObject watermark = new JsonObject();
    watermark.addProperty("value", stamp);
    watermarks.add(watermark);
    return watermarks.toString();
  }

  private static String createJobPayload(List<String> vertexNames) {
    return createJobPayload(vertexNames, n -> UUID.randomUUID().toString());
  }

  private static String createJobPayload(
      List<String> vertexNames, Function<String, String> nameToId) {

    JsonObject obj = new JsonObject();
    obj.addProperty("jid", "dummy-job-id");
    obj.addProperty("name", "dummy-job");
    obj.addProperty("state", "RUNNING");
    JsonArray vertices = new JsonArray();
    for (String vertexName : vertexNames) {
      JsonObject vertex = new JsonObject();
      vertex.addProperty("id", nameToId.apply(vertexName));
      vertex.addProperty("name", vertexName);
      vertices.add(vertex);
    }
    obj.add("vertices", vertices);
    return obj.toString();
  }

  private static Function<URL, String> fromMap(Map<String, String> urlToContent) {
    return fromMap(
        urlToContent,
        u -> {
          throw new IllegalStateException("Missing mapping mapping for " + u);
        });
  }

  private static Function<URL, String> fromMap(
      Map<String, String> urlToContent, Function<URL, String> defaultResult) {

    return u -> {
      String ret = urlToContent.get(u.toString());
      if (ret == null) {
        return defaultResult.apply(u);
      }
      return ret;
    };
  }

  private static FlinkGlobalWatermarkTracker newTracker(Function<URL, String> urlContent) {

    return newTracker(
        urlContent, ImmutableMap.of("rest-address", "http://dummy", "job-name", "dummy-job"));
  }

  private static FlinkGlobalWatermarkTracker newTracker(
      Function<URL, String> urlContent, Map<String, Object> cfgMap) {

    FlinkGlobalWatermarkTracker ret =
        new FlinkGlobalWatermarkTracker() {
          @Override
          JsonElement getJsonForURL(URL url) {
            return JsonParser.parseString(urlContent.apply(url));
          }
        };

    ret.setup(cfgMap);
    return ret;
  }
}
