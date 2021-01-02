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
package cz.o2.proxima.repository.config;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.functional.UnaryPredicate;
import cz.o2.proxima.repository.ConfigConstants;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** Various utilities related to {@link Config}. */
public class ConfigUtils {

  /**
   * Create new {@link Config} from given one with storages of selected attribute families replaced
   * by given URI.
   *
   * @param config input {@link Config} to modify
   * @param familyFilter filter of family names to modify
   * @param storageReplacement function for replacement of URIs
   * @return Config
   */
  public static Config withStorageReplacement(
      Config config,
      UnaryPredicate<String> familyFilter,
      UnaryFunction<String, URI> storageReplacement) {

    final Map<String, String> overrides = new HashMap<>();
    config
        .getObject(ConfigConstants.ATTRIBUTE_FAMILIES)
        .unwrapped()
        .entrySet()
        .stream()
        .filter(e -> familyFilter.apply(e.getKey()))
        .forEach(
            e -> {
              String key = e.getKey();
              Object value = e.getValue();
              @SuppressWarnings("unchecked")
              final Map<String, Object> cast = (Map<String, Object>) value;
              Preconditions.checkState(cast.containsKey(ConfigConstants.STORAGE));
              overrides.put(
                  String.format(
                      "%s.%s.%s", ConfigConstants.ATTRIBUTE_FAMILIES, key, ConfigConstants.STORAGE),
                  storageReplacement.apply(key).toString());
            });
    return ConfigFactory.parseMap(overrides).withFallback(config);
  }

  private ConfigUtils() {}
}
