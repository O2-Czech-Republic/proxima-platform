/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.DataOperator;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.storage.internal.DataAccessorLoader;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link DataOperator} for Apache Beam transformations.
 */
public class BeamDataOperator implements DataOperator {

  private final Repository repo;
  private final DataAccessorLoader<DataAccessorFactory> loader = DataAccessorLoader.of(
      DataAccessorFactory.class);
  private final Map<AttributeFamilyDescriptor, DataAccessor> accessorMap;

  BeamDataOperator(Repository repo) {
    this.repo = repo;
    this.accessorMap = Collections.synchronizedMap(new HashMap<>());
  }

  @Override
  public void close() {
    accessorMap.clear();
  }

  @Override
  public void reload() {
    // nop
  }

  /**
   * Create {@link PCollection} in given {@link Pipeline} from commit log
   * for given attributes.
   * @param pipeline the {@link Pipeline} to create {@link PCollection} in.
   * @param position position in commit log to read from
   * @param stopAtCurrent {@code true} to stop at recent data
   * @param useEventTime {@code true} to use event time
   * @param attrs the attributes to create {@link PCollection} for
   * @return the {@link PCollection}
   */
  @SafeVarargs
  public final PCollection<StreamElement> getStream(
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean useEventTime,
      AttributeDescriptor<?>... attrs) {

    return Arrays
        .stream(attrs)
        .map(desc ->
            repo.getFamiliesForAttribute(desc)
                .stream()
                .filter(af -> af.getAccess().canReadCommitLog())
                // sort primary families on top
                .sorted((l, r) -> Integer.compare(
                    l.getType().ordinal(), r.getType().ordinal()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                    "Missing commit log for " + desc)))
        .distinct()
        .map(this::accessorFor)
        .map(da -> da.getCommitLog(pipeline, position, stopAtCurrent, useEventTime))
        .reduce((left, right) -> Union.of(left, right).output())
        .orElseThrow(() -> new IllegalArgumentException(
            "Pass non empty attribute list"));
  }

  private DataAccessor accessorFor(AttributeFamilyDescriptor family) {

    URI uri = family.getStorageUri();
    return loader.findForUri(uri)
        .map(f -> f.create(family.getEntity(), uri, family.getCfg()))
        .orElseThrow(() -> new IllegalStateException(
            "No accessor for URI " + family.getStorageUri()));
  }

}
