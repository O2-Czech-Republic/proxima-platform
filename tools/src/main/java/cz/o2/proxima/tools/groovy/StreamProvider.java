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
package cz.o2.proxima.tools.groovy;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import java.io.Closeable;
import java.util.function.Predicate;

/** Provider of {@link Stream} based on various parameters. */
public interface StreamProvider extends Closeable {

  @FunctionalInterface
  interface TerminatePredicate {
    boolean check() throws InterruptedException;
  }

  /**
   * Initialize the provider with given repository.
   *
   * @param repo the repository
   * @param args command line arguments passed to {@link Console}
   */
  default void init(Repository repo, String[] args) {}

  /** Close and release all resources. */
  @Override
  void close();

  /**
   * Create stream from commit log(s).
   *
   * @param position position in commit log
   * @param stopAtCurrent {@code true} to stop at current data
   * @param eventTime {@code true} to process using event time
   * @param terminateCheck {@link Predicate} that tests if the execution of any terminal operation
   *     should be interrupted
   * @param attrs attributes to get stream for
   * @return stream from commit log
   */
  Stream<StreamElement> getStream(
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs);

  /**
   * Retrieve batch updates stream.
   *
   * @param startStamp starting stamp (inclusive)
   * @param endStamp ending stamp (exclusive)
   * @param terminateCheck {@link Predicate} that tests if the execution of any terminal operation
   *     should be interrupted
   * @param attrs attributes to read
   * @return globally windowed stream
   */
  WindowedStream<StreamElement> getBatchUpdates(
      long startStamp,
      long endStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs);

  /**
   * Retrieve batch snapshot stream.
   *
   * @param fromStamp starting stamp (inclusive)
   * @param toStamp ending stamp (exclusive)
   * @param terminateCheck {@link Predicate} that tests if the execution of any terminal operation
   *     should be interrupted
   * @param attrs attributes to read
   * @return globally windowed stream
   */
  WindowedStream<StreamElement> getBatchSnapshot(
      long fromStamp,
      long toStamp,
      TerminatePredicate terminateCheck,
      AttributeDescriptor<?>... attrs);
}
