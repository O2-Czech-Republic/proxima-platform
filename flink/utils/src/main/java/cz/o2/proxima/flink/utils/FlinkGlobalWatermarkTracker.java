/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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

import cz.o2.proxima.core.storage.watermark.GlobalWatermarkTracker;
import cz.o2.proxima.core.time.Watermarks;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.collect.Sets;
import cz.o2.proxima.internal.com.google.common.collect.Streams;
import cz.o2.proxima.internal.com.google.gson.JsonArray;
import cz.o2.proxima.internal.com.google.gson.JsonElement;
import cz.o2.proxima.internal.com.google.gson.JsonObject;
import cz.o2.proxima.internal.com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlinkGlobalWatermarkTracker implements GlobalWatermarkTracker {

  private URL flinkMasterRest;
  private String jobName;
  private List<String> vertexNames;
  private @Nullable String jobId;
  private @Nullable List<String> vertices;
  private String name;
  private long updateInterval;
  private long lastUpdate = Watermarks.MIN_WATERMARK;
  private long globalWatermark;

  @Override
  public String getName() {
    return name;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setup(Map<String, Object> cfg) {
    flinkMasterRest =
        Optional.ofNullable(cfg.get("rest-address"))
            .map(String::valueOf)
            .map(s -> ExceptionUtils.uncheckedFactory(() -> new URL(s)))
            .orElseThrow(() -> new IllegalArgumentException("Missing rest-address config option"));
    jobName =
        Optional.ofNullable(cfg.get("job-name"))
            .map(String::valueOf)
            .orElseThrow(() -> new IllegalArgumentException("Missing job-name config option"));
    vertexNames =
        Optional.ofNullable(cfg.get("vertex-names"))
            .map(FlinkGlobalWatermarkTracker::parseVertexNamesToList)
            .orElse(Collections.emptyList());
    name = String.format("%s(%s)e", getClass().getSimpleName(), jobName);
    updateInterval =
        Optional.ofNullable(cfg.get("update-interval-ms"))
            .map(String::valueOf)
            .map(Long::valueOf)
            .orElse(30000L);
    globalWatermark = Watermarks.MAX_WATERMARK;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @VisibleForTesting
  static List<String> parseVertexNamesToList(Object o) {
    if (o instanceof List) {
      return Stream.of(o)
          .map(List.class::cast)
          .flatMap(l -> (Stream<String>) l.stream().map(Object::toString))
          .collect(Collectors.toList());
    }
    return Collections.singletonList(o.toString());
  }

  private List<String> getVertices() {
    return vertexNames.isEmpty() ? readFullVertices() : readVertexIdsFromNames(vertexNames);
  }

  private String getJobId(String jobName) {
    try {
      JsonArray jobs = getJsonForURL(getApiUrl("jobs")).getAsJsonObject().getAsJsonArray("jobs");
      return Streams.stream(jobs)
          .map(JsonElement::getAsJsonObject)
          .map(obj -> obj.get("id").getAsString())
          .map(id -> ExceptionUtils.uncheckedFactory(() -> getJsonForURL(getApiUrl("jobs/" + id))))
          .map(JsonElement::getAsJsonObject)
          .filter(obj -> obj.has("name"))
          .filter(obj -> obj.get("name").getAsString().equals(jobName))
          .filter(obj -> obj.get("state").getAsString().equals("RUNNING"))
          .map(obj -> obj.get("jid").getAsString())
          .findAny()
          .orElseThrow(() -> new IllegalStateException(String.format("Job %s not found", jobName)));
    } catch (IOException e) {
      throw new IllegalStateException("Cannot get job id for " + jobName, e);
    }
  }

  private List<String> readVertexIdsFromNames(List<String> names) {
    Set<String> nameSet = Sets.newHashSet(names);
    return readVertexIds(obj -> nameSet.contains(obj.get("name").getAsString()));
  }

  private List<String> readFullVertices() {
    return readVertexIds(obj -> true);
  }

  private List<String> readVertexIds(Predicate<JsonObject> acceptable) {
    try {
      final JsonObject parsedJson;
      URL rest = getApiUrl("jobs/" + jobId());
      parsedJson = getJsonForURL(rest).getAsJsonObject();
      JsonArray jobVertices = parsedJson.getAsJsonArray("vertices");
      return Streams.stream(jobVertices)
          .map(JsonElement::getAsJsonObject)
          .filter(acceptable)
          .map(el -> el.get("id").getAsString())
          .collect(Collectors.toList());
    } catch (IOException ex) {
      throw new IllegalStateException("Cannot read vertex ids", ex);
    }
  }

  private String jobId() {
    if (jobId == null) {
      jobId = getJobId(jobName);
    }
    return jobId;
  }

  private List<String> vertices() {
    if (vertices == null) {
      vertices = getVertices();
    }
    return vertices;
  }

  @Override
  public void initWatermarks(Map<String, Long> initialWatermarks) {
    // nop
  }

  @Override
  public CompletableFuture<Void> update(String processName, long currentWatermark) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void finished(String name) {
    // nop
  }

  @Override
  public long getGlobalWatermark(@Nullable String processName, long currentWatermark) {
    if (System.currentTimeMillis() > updateInterval + lastUpdate) {
      updateGlobalWatermark();
    }
    return globalWatermark;
  }

  private void updateGlobalWatermark() {
    globalWatermark = getMinWatermarkFrom(vertices());
    lastUpdate = System.currentTimeMillis();
  }

  @VisibleForTesting
  long getMinWatermarkFrom(List<String> vertices) {
    return vertices.stream()
        .map(v -> getApiUrl("jobs/" + jobId() + "/vertices/" + v + "/watermarks"))
        .map(url -> ExceptionUtils.uncheckedFactory(() -> getJsonForURL(url)))
        .map(JsonElement::getAsJsonArray)
        .flatMapToLong(
            arr -> Streams.stream(arr).mapToLong(o -> o.getAsJsonObject().get("value").getAsLong()))
        .min()
        .orElse(Watermarks.MAX_WATERMARK);
  }

  @VisibleForTesting
  JsonElement getJsonForURL(URL url) throws IOException {
    try (InputStream in = url.openConnection().getInputStream()) {
      return parseInputStreamToJson(in);
    }
  }

  @VisibleForTesting
  static JsonElement parseInputStreamToJson(InputStream in) {
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    return JsonParser.parseString(reader.lines().collect(Collectors.joining("\n")));
  }

  private URL getApiUrl(String apiPath) {
    return getApiUrlFor(flinkMasterRest, apiPath);
  }

  @VisibleForTesting
  static URL getApiUrlFor(URL base, String apiPath) {
    try {
      return new URL(base, apiPath);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(
          String.format("Cannot form valid URL from %s and %s", base.toString(), apiPath), e);
    }
  }
}
