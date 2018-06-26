/**
 * Copyright 2017-2018 O2 Czech Republic, a.s.
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
package cz.o2.proxima.transform;

import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;

/**
 * Transform perfoming identity mapping.
 */
public class IdentityTransform implements Transformation, ProxyTransform {

  @Override
  public void setup(Repository repo) {

  }

  @Override
  public int apply(StreamElement input, Collector<StreamElement> collector) {
    collector.collect(input);
    return 1;
  }

  @Override
  public String fromProxy(String proxy) {
    return proxy;
  }

  @Override
  public String toProxy(String raw) {
    return raw;
  }

}
