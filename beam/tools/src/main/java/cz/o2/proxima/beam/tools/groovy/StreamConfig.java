/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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
import java.io.Serializable;
import java.net.InetAddress;

/** */
public class StreamConfig implements Serializable {

  private static final String COLLECT_PORT_KEY = "console.collect.server-port";
  private static final String COLLECT_HOSTNAME = "console.collect.hostname";

  static StreamConfig empty() {
    return new StreamConfig(ConfigFactory.empty());
  }

  static StreamConfig of(BeamDataOperator beam) {
    return new StreamConfig(((ConfigRepository) beam.getRepository()).getConfig());
  }

  private final int collectPort;
  private final String collectHostname;

  private StreamConfig(Config config) {
    try {
      this.collectPort = config.hasPath(COLLECT_PORT_KEY) ? config.getInt(COLLECT_PORT_KEY) : -1;
      this.collectHostname =
          isNonEmpty(config, COLLECT_HOSTNAME)
              ? config.getString(COLLECT_HOSTNAME)
              : InetAddress.getLocalHost().getHostName();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private static boolean isNonEmpty(Config config, String path) {
    return config.hasPath(path) && !config.getString(path).isEmpty();
  }

  int getPreferredCollectPort() {
    return collectPort;
  }

  String getCollectHostname() {
    return collectHostname;
  }
}
