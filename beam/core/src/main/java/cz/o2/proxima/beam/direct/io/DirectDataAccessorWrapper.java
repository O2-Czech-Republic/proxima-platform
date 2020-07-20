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
package cz.o2.proxima.beam.direct.io;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import cz.o2.proxima.beam.core.DataAccessor;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.batch.BatchLogObservable;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Wrapper of direct data accessor to beam one. */
public class DirectDataAccessorWrapper implements DataAccessor {

  private static final long serialVersionUID = 1L;
  private static final String CONFIG_PREFIX = "beam.";
  private static final String UNBOUNDED_BATCH_SOURCE_PREFIX = CONFIG_PREFIX + "unbounded-batch";

  interface ConfigReader extends Serializable {
    /**
     * Return bytes per second allowed throughput to be read from {@link
     * DirectBatchUnboundedSource}.
     */
    long getBytesPerSecThroughput(Repository repository);
  }

  private final RepositoryFactory factory;
  private final cz.o2.proxima.direct.core.DataAccessor direct;
  private final URI uri;
  private final Context context;

  public DirectDataAccessorWrapper(
      Repository repo, cz.o2.proxima.direct.core.DataAccessor direct, URI uri, Context context) {

    this.factory = repo.asFactory();
    this.direct = direct;
    this.uri = uri;
    this.context = context;
  }

  @Override
  public PCollection<StreamElement> createStream(
      String name,
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      long limit) {

    CommitLogReader reader =
        direct
            .getCommitLogReader(context)
            .orElseThrow(
                () -> new IllegalArgumentException("Cannot create commit log from " + direct));

    final PCollection<StreamElement> ret;
    if (stopAtCurrent) {
      // bounded
      ret =
          pipeline.apply(
              "ReadBounded:" + uri,
              Read.from(DirectBoundedSource.of(factory, name, reader, position, limit)));
    } else {
      // unbounded
      ret =
          pipeline.apply(
              "ReadUnbounded:" + uri,
              Read.from(
                  DirectUnboundedSource.of(factory, name, reader, position, eventTime, limit)));
    }
    return ret.setCoder(StreamElementCoder.of(factory))
        .setTypeDescriptor(TypeDescriptor.of(StreamElement.class));
  }

  @Override
  public PCollection<StreamElement> createBatch(
      Pipeline pipeline, List<AttributeDescriptor<?>> attrs, long startStamp, long endStamp) {

    BatchLogObservable reader =
        direct
            .getBatchLogObservable(context)
            .orElseThrow(
                () ->
                    new IllegalArgumentException("Cannot create batch observable from " + direct));

    PCollection<StreamElement> ret =
        pipeline.apply(
            "ReadBoundedBatch:" + uri,
            Read.from(DirectBatchSource.of(factory, reader, attrs, startStamp, endStamp)));

    ret.setTypeDescriptor(TypeDescriptor.of(StreamElement.class))
        .setCoder(StreamElementCoder.of(factory));

    return AssignEventTime.of(ret)
        .using(StreamElement::getStamp)
        .output()
        .setCoder(ret.getCoder())
        .setTypeDescriptor(TypeDescriptor.of(StreamElement.class));
  }

  @Override
  public PCollection<StreamElement> createStreamFromUpdates(
      Pipeline pipeline,
      List<AttributeDescriptor<?>> attrs,
      long startStamp,
      long endStamp,
      long limit) {

    BatchLogObservable reader =
        direct
            .getBatchLogObservable(context)
            .orElseThrow(
                () ->
                    new IllegalArgumentException("Cannot create batch observable from " + direct));

    final PCollection<StreamElement> ret;
    ret =
        pipeline.apply(
            "ReadBatchUnbounded:" + uri,
            Read.from(
                DirectBatchUnboundedSource.of(
                    factory, reader, getConfigProvider(uri), attrs, startStamp, endStamp)));
    return ret.setCoder(StreamElementCoder.of(factory))
        .setTypeDescriptor(TypeDescriptor.of(StreamElement.class));
  }

  @VisibleForTesting
  static ConfigReader getConfigProvider(URI uri) {
    return repo -> {
      Config config =
          repo instanceof ConfigRepository ? ((ConfigRepository) repo).getConfig() : null;
      return readThroughput(uri, config);
    };
  }

  @VisibleForTesting
  static long readThroughput(URI uri, @Nullable Config config) {
    if (config != null && config.hasPath(UNBOUNDED_BATCH_SOURCE_PREFIX)) {
      ConfigObject object = config.getObject(UNBOUNDED_BATCH_SOURCE_PREFIX);
      String uriString = uri.toString();
      for (Map.Entry<String, ConfigValue> entry : object.entrySet()) {
        if (entry.getValue().valueType() == ConfigValueType.OBJECT) {
          @SuppressWarnings({"unchecked", "rawtypes"})
          Map<String, Object> obj = (Map) entry.getValue().unwrapped();
          Object sourceUri = obj.get("uri");
          if (sourceUri != null && uriString.equals(sourceUri.toString())) {
            Object throughput =
                Objects.requireNonNull(obj.get("throughput"), "Missing key `throughput` in " + obj);
            return Long.parseLong(throughput.toString());
          }
        }
      }
    }
    return -1L;
  }
}
