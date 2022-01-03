/**
 * Copyright 2017-2022 O2 Czech Republic, a.s.
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
package cz.o2.proxima.server;

/** Various (mostly config related) constants. */
public class Constants {

  public static final int CORES = Math.max(2, Runtime.getRuntime().availableProcessors());

  public static final String CFG_IGNORE_ERRORS = "ingest.ignore-errors";
  public static final String CFG_PORT = "ingest.server.port";
  public static final int DEFAULT_PORT = 4001;
  public static final String CFG_NUM_THREADS = "ingest.server.num-threads";
  public static final int DEFAULT_NUM_THREADS = 10 * CORES;

  private Constants() {
    // nop
  }
}
