/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core;

import cz.o2.proxima.core.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.core.repository.Repository;
import cz.o2.proxima.direct.core.batch.BatchLogReader;
import cz.o2.proxima.direct.core.commitlog.CommitLogReader;
import cz.o2.proxima.direct.core.randomaccess.RandomAccessReader;
import cz.o2.proxima.direct.core.view.CachedView;
import java.util.Optional;

class InvalidDirectAttributeFamilyDescriptor extends DirectAttributeFamilyDescriptor {
  public InvalidDirectAttributeFamilyDescriptor(Repository repo, AttributeFamilyDescriptor family) {
    super(
        repo,
        family,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  @Override
  public Optional<AttributeWriterBase> getWriter() {
    failUnsupported();
    return super.getWriter();
  }

  private void failUnsupported() {
    throw new IllegalArgumentException(
        String.format(
            "No provider of storage of %s. You might be missing some dependency.",
            getDesc().getStorageUri()));
  }

  @Override
  public Optional<CommitLogReader> getCommitLogReader() {
    failUnsupported();
    return super.getCommitLogReader();
  }

  @Override
  public Optional<BatchLogReader> getBatchReader() {
    failUnsupported();
    return super.getBatchReader();
  }

  @Override
  public Optional<RandomAccessReader> getRandomAccessReader() {
    failUnsupported();
    return super.getRandomAccessReader();
  }

  @Override
  public Optional<CachedView> getCachedView() {
    failUnsupported();
    return super.getCachedView();
  }

  @Override
  boolean hasBatchReader() {
    failUnsupported();
    return super.hasBatchReader();
  }

  @Override
  boolean hasCachedView() {
    failUnsupported();
    return super.hasCachedView();
  }

  @Override
  boolean hasCommitLogReader() {
    failUnsupported();
    return super.hasCommitLogReader();
  }

  @Override
  boolean hasRandomAccessReader() {
    failUnsupported();
    return super.hasRandomAccessReader();
  }
}
