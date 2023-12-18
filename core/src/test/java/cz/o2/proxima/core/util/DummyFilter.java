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
package cz.o2.proxima.core.util;

import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.core.storage.PassthroughFilter;
import cz.o2.proxima.core.storage.StreamElement;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.util.Map;
import lombok.Getter;

/** A dummy filter just to be able to test difference from {@link PassthroughFilter}. */
public class DummyFilter extends PassthroughFilter {

  @Getter private boolean setupCalled = false;

  @Override
  public void setup(Repository repository, Map<String, Object> cfg) {
    Preconditions.checkState(!setupCalled, "Filter setup should be called just once!");
    setupCalled = true;
  }

  @Override
  public boolean apply(StreamElement element) {
    Preconditions.checkState(setupCalled, "Filter setup should be called before apply!");
    return super.apply(element);
  }
}
