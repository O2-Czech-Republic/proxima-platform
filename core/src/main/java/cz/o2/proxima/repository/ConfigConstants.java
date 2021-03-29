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
package cz.o2.proxima.repository;

/** Constants related to config parsing. */
public class ConfigConstants {

  // config parsing constants
  public static final String VALIDATIONS = "validations";
  public static final String ALL = "all";
  public static final String LOCAL = "local";
  public static final String READ = "read";
  public static final String WRITE = "write";
  public static final String ATTRIBUTES = "attributes";
  public static final String ENTITY = "entity";
  public static final String VIA = "via";
  public static final String SOURCE = "source";
  public static final String TARGETS = "targets";
  public static final String READ_ONLY = "read-only";
  public static final String COMMIT_LOG = "commit-log";
  public static final String STATE_COMMIT_LOG = "state-commit-log";
  public static final String WRITE_ONLY = "write-only";
  public static final String BATCH_UPDATES = "batch-updates";
  public static final String BATCH_SNAPSHOT = "batch-snapshot";
  public static final String RANDOM_ACCESS = "random-access";
  public static final String LIST_PRIMARY_KEY = "list-primary-key";
  public static final String CACHED_VIEW = "cached-view";
  public static final String REPLICATIONS = "replications";
  public static final String DISABLED = "disabled";
  public static final String ENTITIES = "entities";
  public static final String SCHEME = "scheme";
  public static final String PROXY = "proxy";
  public static final String FROM = "from";
  public static final String STORAGE = "storage";
  public static final String ACCESS = "access";
  public static final String TYPE = "type";
  public static final String FILTER = "filter";
  public static final String ATTRIBUTE_FAMILIES = "attributeFamilies";
  public static final String TRANSACTIONAL = "transactional";
  public static final String MANAGER = "manager";
  public static final String TRANSACTIONS = "transactions";
  public static final String SCHEME_PROVIDER = "scheme-provider";

  private ConfigConstants() {
    // nop
  }
}
