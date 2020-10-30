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

import cz.o2.proxima.beam.core.DataAccessor;
import cz.o2.proxima.beam.core.io.StreamElementCoder;
import cz.o2.proxima.direct.batch.BatchLogReader;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.Context;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.net.URI;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Wrapper of direct data accessor to beam one. */
public class DirectDataAccessorWrapper implements DataAccessor {

  private static final long serialVersionUID = 1L;

  private final RepositoryFactory factory;
  private final cz.o2.proxima.direct.core.DataAccessor direct;
  @Getter private final URI uri;
  private final Context context;
  private final Map<String, Object> cfg;

  public DirectDataAccessorWrapper(
      Repository repo,
      cz.o2.proxima.direct.core.DataAccessor direct,
      URI uri,
      Context context,
      Map<String, Object> cfg) {

    this.factory = repo.asFactory();
    this.direct = direct;
    this.uri = uri;
    this.context = context;
    this.cfg = cfg;
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

    BatchLogReader reader =
        direct
            .getBatchLogReader(context)
            .orElseThrow(
                () -> new IllegalArgumentException("Cannot create batch reader from " + direct));

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

    BatchLogReader reader =
        direct
            .getBatchLogReader(context)
            .orElseThrow(
                () -> new IllegalArgumentException("Cannot create batch reader from " + direct));

    final PCollection<StreamElement> ret;
    ret =
        pipeline.apply(
            "ReadBatchUnbounded:" + uri,
            Read.from(
                DirectBatchUnboundedSource.of(factory, reader, attrs, startStamp, endStamp, cfg)));
    return ret.setCoder(StreamElementCoder.of(factory))
        .setTypeDescriptor(TypeDescriptor.of(StreamElement.class));
  }
}
