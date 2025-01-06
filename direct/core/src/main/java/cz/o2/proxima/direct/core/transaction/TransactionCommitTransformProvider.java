/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core.transaction;

import com.google.auto.service.AutoService;
import cz.o2.proxima.core.annotations.Internal;
import cz.o2.proxima.core.transaction.TransactionTransformProvider;
import cz.o2.proxima.core.transform.Transformation;

@Internal
@AutoService(TransactionTransformProvider.class)
public class TransactionCommitTransformProvider implements TransactionTransformProvider {

  @Override
  public Transformation create() {
    return new TransactionCommitTransformation();
  }
}
