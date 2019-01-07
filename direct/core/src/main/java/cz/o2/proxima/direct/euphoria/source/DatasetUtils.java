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
package cz.o2.proxima.direct.euphoria.source;

import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.direct.commitlog.CommitLogReader;
import cz.o2.proxima.direct.commitlog.Position;
import cz.o2.proxima.direct.core.Context;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.Filter;
import cz.seznam.euphoria.core.client.operator.Union;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities able to retrieve {@link Dataset} for given attribute(s).
 */
@Internal
public class DatasetUtils {

  /**
   * Retrieve {@link Dataset} that contains given attributes with given read
   * specification.
   * @param flow flow to add this {@link Dataset} to
   * @param repo the repository
   * @param context context of direct module
   * @param position position where to start reading
   * @param stopAtCurrent should we stop processing when current data reached?
   * @param attrs the attributes to read
   * @return {@link Dataset} that contains given attributes
   */
  public static Dataset<StreamElement> of(
      Flow flow,
      Repository repo,
      Context context,
      Position position,
      boolean stopAtCurrent,
      AttributeDescriptor<?>... attrs) {

    return stream(flow, repo, context, position, asSet(attrs), stopAtCurrent);
  }

  @SuppressWarnings("unchecked")
  private static Dataset<StreamElement> stream(
      Flow flow,
      Repository repo,
      Context context,
      Position position,
      Set<AttributeDescriptor> attrs,
      boolean stopAtCurrent) {

    Set<CommitLogReader> readers = new HashSet<>();
    for (AttributeDescriptor a : attrs) {
      readers.add(repo.getFamiliesForAttribute(a).stream()
          .filter(af -> af.getAccess().canReadCommitLog())
          .map(af -> context.resolve(af).orElseThrow(
              () -> new IllegalStateException("Missing direct wrapper of " + af)))
          .map(af -> af.getCommitLogReader().get())
          .findAny()
          .orElseThrow(() -> new IllegalArgumentException(
              "Attribute " + a + " has no commit log")));
    }
    List<Dataset<StreamElement>> inputs = readers.stream()
        .map(r -> stopAtCurrent
            ? BoundedStreamSource.of(r, position)
            : UnboundedStreamSource.of(r, position))
        .map(flow::createInput)
        .collect(Collectors.toList());
    final Dataset<StreamElement> united;
    if (inputs.size() > 1) {
      united = Union.of(inputs).output();
    } else {
      united = inputs.get(0);
    }
    return Filter.of(united)
        .by(e -> attrs.contains(e.getAttributeDescriptor()))
        .output();
  }

  @SuppressWarnings("unchecked")
  private static Set<AttributeDescriptor> asSet(AttributeDescriptor<?>[] attrs) {
    return Arrays.stream(attrs).collect(Collectors.toSet());
  }

  private DatasetUtils() { }

}
