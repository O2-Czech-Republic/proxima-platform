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
package cz.o2.proxima.storage;

import cz.o2.proxima.annotations.Stable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** A filter that is applied to each input data ingest. */
@Stable
@FunctionalInterface
public interface StorageFilter extends Serializable {

  /** Filter consisting of several filters with applied function. */
  public abstract class CompoundFilter implements StorageFilter {

    protected final List<StorageFilter> filters = new ArrayList<>();

    CompoundFilter(List<StorageFilter> filters) {
      this.filters.addAll(filters);
    }
  }

  /** Filter performing logical OR of several filters. */
  public class OrFilter extends CompoundFilter {

    protected OrFilter(List<StorageFilter> filters) {
      super(filters);
    }

    @Override
    public boolean apply(StreamElement ingest) {
      return filters.stream().anyMatch(f -> f.apply(ingest));
    }
  }

  /** Filter performing logical AND of several filters. */
  public class AndFilter extends CompoundFilter {

    protected AndFilter(List<StorageFilter> filters) {
      super(filters);
    }

    @Override
    public boolean apply(StreamElement ingest) {
      return filters.stream().allMatch(f -> f.apply(ingest));
    }
  }

  /**
   * When returns {@code false} the input element is not stored in the storage and is throws away.
   *
   * @param ingest the input data
   * @return {@code false} to throw the element away
   */
  boolean apply(StreamElement ingest);
}
