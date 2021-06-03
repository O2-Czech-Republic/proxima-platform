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
package cz.o2.proxima.flink.core;

import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import java.io.Serializable;

/**
 * Simple function, that extracts {@link StreamElement} into a desired type.
 *
 * @param <T> Type to extract.
 */
@FunctionalInterface
public interface ResultExtractor<T> extends Serializable {

  /**
   * Create identity extractor, that just returns an input element.
   *
   * @return Identity extractor.
   */
  static ResultExtractor<StreamElement> identity() {
    return el -> el;
  }

  /**
   * Create an extractor, that just extracts parsed value from a stream element.
   *
   * @param <T> Type of extracted element.
   * @return Parsed value.
   */
  static <T> ResultExtractor<T> parsed() {
    return el -> {
      @SuppressWarnings("unchecked")
      final T parsed = (T) Optionals.get(el.getParsed());
      return parsed;
    };
  }

  /**
   * Convert {@link StreamElement} into a desired type.
   *
   * @param element Element to convert.
   * @return Converted element.
   */
  T toResult(StreamElement element);
}
