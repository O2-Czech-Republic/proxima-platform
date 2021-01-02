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
package cz.o2.proxima.beam.transforms;

import cz.o2.proxima.storage.StreamElement;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/** Extract key from {@link StreamElement} into KV. */
public class ExtractKeyToKv {

  public static PTransform<PCollection<StreamElement>, PCollection<KV<String, StreamElement>>>
      fromStreamElements() {
    return new ExtractKeyToKvFromStreamElement();
  }

  private static class ExtractKeyToKvFromStreamElement
      extends PTransform<PCollection<StreamElement>, PCollection<KV<String, StreamElement>>> {
    @Override
    public PCollection<KV<String, StreamElement>> expand(PCollection<StreamElement> input) {
      return input.apply(
          MapElements.into(
                  TypeDescriptors.kvs(
                      TypeDescriptors.strings(), TypeDescriptor.of(StreamElement.class)))
              .via(el -> KV.of(el.getKey(), el)));
    }
  }

  private ExtractKeyToKv() {}
}
