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
package cz.o2.proxima.beam.tools.groovy;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.beam.core.BeamDataOperator;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import java.io.Serializable;
import java.net.InetAddress;
import lombok.Getter;

/** Configuration object for {@link BeamStream}. */
public class StreamConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String COLLECT_PORT_KEY = "console.collect.server-port";
  private static final String COLLECT_HOSTNAME = "console.collect.hostname";

  static StreamConfig empty() {
    return new StreamConfig((ConfigRepository) Repository.of(ConfigFactory.empty()));
  }

  static StreamConfig of(BeamDataOperator beam) {
    return new StreamConfig(((ConfigRepository) beam.getRepository()));
  }

  @Getter private final int collectPort;
  @Getter private final String collectHostname;
  private final RepositoryFactory repositoryFactory;
  private transient Repository repo;

  private StreamConfig(ConfigRepository repository) {
    try {
      Config config = repository.getConfig();
      this.collectPort = config.hasPath(COLLECT_PORT_KEY) ? config.getInt(COLLECT_PORT_KEY) : -1;
      this.collectHostname =
          isNonEmpty(config, COLLECT_HOSTNAME)
              ? config.getString(COLLECT_HOSTNAME)
              : InetAddress.getLocalHost().getHostName();
      this.repositoryFactory = repository.asFactory();
      this.repo = repository;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  public Repository getRepo() {
    if (repo == null) {
      repo = repositoryFactory.apply();
    }
    return repo;
  }

  private static boolean isNonEmpty(Config config, String path) {
    return config.hasPath(path) && !config.getString(path).isEmpty();
  }
}
