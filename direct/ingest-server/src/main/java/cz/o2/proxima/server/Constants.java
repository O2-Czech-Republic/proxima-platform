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
package cz.o2.proxima.server;

/** Various (mostly config related) constants. */
public class Constants {

  public static final String CFG_IGNORE_ERRORS = "ingest.ignore-errors";
  public static final String CFG_PORT = "ingest.server.port";
  public static final int DEFALT_PORT = 4001;

  private Constants() {
    // nop
  }
}
