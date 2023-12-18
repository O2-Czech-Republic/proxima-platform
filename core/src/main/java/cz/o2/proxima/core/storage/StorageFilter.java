/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.storage;

import cz.o2.proxima.core.annotations.Stable;
import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.repository.TransformationDescriptor;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A filter that is applied to each input element.
 *
 * <p>Filter can be used for {@link AttributeFamilyDescriptor} and/or {@link
 * TransformationDescriptor}
 */
@Stable
@FunctionalInterface
public interface StorageFilter extends Serializable {

  /** Filter consisting of several filters with applied function. */
  abstract class CompoundFilter implements StorageFilter {

    protected final List<StorageFilter> filters = new ArrayList<>();

    CompoundFilter(List<StorageFilter> filters) {
      this.filters.addAll(filters);
    }
  }

  /** Filter performing logical OR of several filters. */
  class OrFilter extends CompoundFilter {

    protected OrFilter(List<StorageFilter> filters) {
      super(filters);
    }

    @Override
    public boolean apply(StreamElement element) {
      return filters.stream().anyMatch(f -> f.apply(element));
    }
  }

  /** Filter performing logical AND of several filters. */
  class AndFilter extends CompoundFilter {

    protected AndFilter(List<StorageFilter> filters) {
      super(filters);
    }

    @Override
    public boolean apply(StreamElement element) {
      return filters.stream().allMatch(f -> f.apply(element));
    }
  }

  /**
   * When returns {@code false} the input element is not stored in the storage and is discarded.
   *
   * @param element the input data
   * @return {@code false} to throw the element away
   */
  boolean apply(StreamElement element);

  /**
   * Setup filter
   *
   * @param repository repository
   * @param cfg configuration map
   */
  default void setup(Repository repository, Map<String, Object> cfg) {
    // default no-op
  }
}
