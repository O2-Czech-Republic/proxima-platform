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
package cz.o2.proxima.repository;

/**
 * Various (mostly config related) constants.
 */
public class Constants {

  public static final String EXECUTOR_POOL_SIZE_CFG = "proxima.executor.size";
  public static final int EXECUTOR_POOL_SIZE_DEFAULT = 10 * Runtime.getRuntime().availableProcessors();
  public static final String EXECUTOR_POOL_SIZE_CFG_DESC =
      "Default size of ScheduledExecutorService used for executing tasks";

  private Constants() { }

}
