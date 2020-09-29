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
package cz.o2.proxima.beam.storage;

import com.typesafe.config.Config;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.beam.core.DataAccessor;
import cz.o2.proxima.beam.core.DataAccessorFactory;
import cz.o2.proxima.functional.UnaryPredicate;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.config.ConfigUtils;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;

/**
 * Storage with URI scheme {@code test-stream://}. This storage is backed by {@link TestStream} and
 * enables fine tuning with regard to watermark and other stream aspects.
 */
public class TestStreamStorage implements DataAccessorFactory {

  private static final Map<Repository, StreamProviders> storages = new ConcurrentHashMap<>();

  private static class StreamProviders {
    private final Map<URI, TestStream<StreamElement>> streams = new HashMap<>();

    TestStream<StreamElement> get(URI uri) {
      return Objects.requireNonNull(streams.get(uri));
    }

    void put(URI uri, TestStream<StreamElement> stream) {
      streams.put(uri, stream);
    }
  }

  /**
   * Replace all storages in all attribute families in given config with {@code test-stream}.
   *
   * @param config the configuration to replace
   * @return replaced config
   */
  public static Config replaceStorages(Config config) {
    return replaceStorages(config, name -> true);
  }

  /**
   * Replace all storages in all attribute families in given config with {@code test-stream}.
   *
   * @param config the configuration to replace
   * @param familyFilter filter for families to replace storage in
   * @return replaced config
   */
  public static Config replaceStorages(Config config, UnaryPredicate<String> familyFilter) {
    return ConfigUtils.withStorageReplacement(
        config, familyFilter, name -> URI.create(String.format("test-stream://%s", name)));
  }

  /**
   * Put given {@link TestStream} for given {@link AttributeFamilyDescriptor}.
   *
   * @param repo repository
   * @param family attribute family descriptor
   * @param stream the stream to use as data source
   */
  public static void putStream(
      Repository repo, AttributeFamilyDescriptor family, TestStream<StreamElement> stream) {
    StreamProviders providers = storages.computeIfAbsent(repo, k -> new StreamProviders());
    providers.put(URI.create(String.format("test-stream://%s", family.getName())), stream);
  }

  private Repository repo;

  @Override
  public void setup(Repository repo) {
    this.repo = Objects.requireNonNull(repo);
  }

  @Override
  public Accept accepts(URI uri) {
    return uri.getScheme().equals("test-stream") ? Accept.ACCEPT : Accept.REJECT;
  }

  @Override
  public DataAccessor createAccessor(
      BeamDataOperator operator, EntityDescriptor entity, URI uri, Map<String, Object> cfg) {

    return new DataAccessor() {

      @Override
      public URI getUri() {
        return uri;
      }

      @Override
      public PCollection<StreamElement> createStream(
          String name,
          Pipeline pipeline,
          Position position,
          boolean stopAtCurrent,
          boolean eventTime,
          long limit) {

        return streamFor(pipeline, uri);
      }

      @Override
      public PCollection<StreamElement> createStreamFromUpdates(
          Pipeline pipeline,
          List<AttributeDescriptor<?>> attrs,
          long startStamp,
          long endStamp,
          long limit) {

        return streamFor(pipeline, uri);
      }

      @Override
      public PCollection<StreamElement> createBatch(
          Pipeline pipeline, List<AttributeDescriptor<?>> attrs, long startStamp, long endStamp) {

        return streamFor(pipeline, uri);
      }
    };
  }

  private PCollection<StreamElement> streamFor(Pipeline pipeline, URI uri) {
    return pipeline.apply(providers().get(uri));
  }

  private StreamProviders providers() {
    return Objects.requireNonNull(storages.get(repo));
  }
}
