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
package cz.o2.proxima.beam.core;

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import java.util.Comparator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/** Various tools related to manipulation with {@link PCollection}s. */
public class PCollectionTools {

  /**
   * Reduce given {@link PCollection} from updates to snapshot.
   *
   * @param name name of the operation
   * @param other the other {@link PCollection} containing updates
   * @return snapshot
   */
  public static PCollection<StreamElement> reduceAsSnapshot(
      @Nullable String name, PCollection<StreamElement> other) {

    return ReduceByKey.named(name)
        .of(other)
        .keyBy(e -> e.getKey() + "#" + e.getAttribute(), TypeDescriptors.strings())
        .valueBy(e -> e, TypeDescriptor.of(StreamElement.class))
        .combineBy(
            values -> Optionals.get(values.max(Comparator.comparingLong(StreamElement::getStamp))),
            TypeDescriptor.of(StreamElement.class))
        .outputValues();
  }

  private PCollectionTools() {}
}
