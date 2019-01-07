/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.view;

import cz.o2.proxima.annotations.Stable;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.direct.core.Partition;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * A view of a stream data that can be observed in through partitions.
 */
@Stable
public interface PartitionedView extends Serializable {

  /**
   * Retrieve entity associated with the view.
   * @return entity associated with the view
   */
  EntityDescriptor getEntityDescriptor();

  /**
   * Retrieve list of partitions of this commit log.
   * @return list of partitions in this view
   */
  List<Partition> getPartitions();

  /**
   * Subscribe to given set of partitions.
   * If you use this call then the reader stops being automatically
   * load balanced and the set of partitions cannot be changed.
   * @param <T> output data type
   * @param flow the flow to run this observation in
   * @param partitions the list of partitions to subscribe to
   * @param observer the observer to subscribe to the partitions
   * @return {@link Dataset} produced by this observer
   */
  <T> Dataset<T> observePartitions(
      Flow flow,
      Collection<Partition> partitions,
      PartitionedLogObserver<T> observer);


  /**
   * Subscribe to given set of partitions.
   * If you use this call then the reader stops being automatically
   * load balanced and the set of partitions cannot be changed.
   * @param <T> output datatype
   * @param partitions the list of partitions to subscribe to
   * @param observer the observer to subscribe to the partitions
   * @return {@link Dataset} produced by this observer
   */
  default <T> Dataset<T> observePartitions(
      Collection<Partition> partitions,
      PartitionedLogObserver<T> observer) {

    return observePartitions(Flow.create(), partitions, observer);
  }


  /**
   * Subscribe observer by name and read the newest data.
   * Each observer maintains its own position in the commit log, so that
   * the observers with different names do not interfere
   * If multiple observers share the same name, then the data
   * is load-balanced between them (in an undefined manner).
   * This is a non blocking call.
   * @param <T> output data type
   * @param flow the flow to observe the data in
   * @param name identifier of the consumer
   * @param observer the observer to subscribe to the commit log
   * @return {@link Dataset} produced by this observer
   */
  <T> Dataset<T> observe(
      Flow flow,
      String name,
      PartitionedLogObserver<T> observer);


  /**
   * Subscribe observer by name and read the newest data.
   * Each observer maintains its own position in the commit log, so that
   * the observers with different names do not interfere
   * If multiple observers share the same name, then the data
   * is load-balanced between them (in an undefined manner).
   * This is a non blocking call.
   * @param <T> output data type
   * @param name name of the observer
   * @param observer the observer
   * @return {@link Dataset} produced by this observer
   */
  default <T> Dataset<T> observe(
      String name, PartitionedLogObserver<T> observer) {

    return observe(Flow.create(), name, observer);
  }


}
