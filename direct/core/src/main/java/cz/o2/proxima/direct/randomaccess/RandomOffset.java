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
package cz.o2.proxima.direct.randomaccess;

import cz.o2.proxima.annotations.Stable;
import java.io.Serializable;

/**
 * An interface representing offset for paging. This interface is needed because various db engines
 * can have different notion of ordering and therefore it might be difficult to do paging based
 * simply on the key (of entity or attribute). Simple example is a hash map, where you cannot page
 * through the map based on the key stored in the map. This is just a labeling interface.
 */
@Stable
public interface RandomOffset extends Serializable {}
