/*
 * Copyright 2017-2025 O2 Czech Republic, a.s.
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
package cz.o2.proxima.core.repository;

import cz.o2.proxima.core.annotations.Experimental;
import cz.o2.proxima.core.util.ExceptionUtils;
import cz.o2.proxima.core.util.internal.ClassLoaders.ChildFirstURLClassLoader;
import cz.o2.proxima.core.util.internal.ClassLoaders.ChildLayerFirstClassLoader;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Class that is used as entry point for modularized applications. The entry point reads a directory
 * of modules (jars), creates a {@link ModuleLayer} from this directory and runs provided class
 * (defined in the layer).
 */
@Experimental
@Slf4j
public class JPMSMain {

  public static void main(String[] args)
      throws ClassNotFoundException,
          NoSuchMethodException,
          InvocationTargetException,
          IllegalAccessException {
    if (args.length < 2) {
      usage(args);
    }
    final Map<String, ClassLoader> loaderCache = new HashMap<>();
    final String path = args[0];
    final String mainClass = args[1];
    final String[] remainingArgs = new String[args.length - 2];
    Class<?> thisClass = JPMSMain.class;
    System.arraycopy(args, 2, remainingArgs, 0, args.length - 2);

    ModuleLayer parentLayer = thisClass.getModule().getLayer();
    if (parentLayer == null) {
      parentLayer = ModuleLayer.boot();
    }
    ModuleFinder finder = ModuleFinder.of(Path.of(path));
    ChildLayerFirstClassLoader parentLoader =
        new ChildLayerFirstClassLoader(finder, thisClass.getClassLoader());
    createModuleLayer(loaderCache, parentLayer, finder, parentLoader);
    log.info("Added module layer from path {} with modules {}", path, finder.findAll());
    Repository.setJpmsClassLoader(parentLoader);
    Thread.currentThread().setContextClassLoader(parentLoader);
    Class<?> loadedMainClass = parentLoader.loadClass(mainClass);
    Method main = loadedMainClass.getDeclaredMethod("main", String[].class);
    main.invoke(null, (Object) remainingArgs);
  }

  private static void createModuleLayer(
      Map<String, ClassLoader> loaderCache,
      ModuleLayer parentLayer,
      ModuleFinder finder,
      ChildLayerFirstClassLoader parentLoader) {

    Configuration parentConf = parentLayer.configuration();
    Configuration moduleConf =
        parentConf.resolveAndBind(
            finder,
            ModuleFinder.of(),
            finder.findAll().stream().map(r -> r.descriptor().name()).collect(Collectors.toList()));
    ModuleLayer layer =
        parentLayer.defineModules(
            moduleConf,
            name ->
                getOrCreateModuleLoader(
                    loaderCache,
                    parentLoader,
                    finder
                        .find(name)
                        .orElseThrow()
                        .location()
                        .map(u -> ExceptionUtils.uncheckedFactory(u::toURL))
                        .orElseThrow(),
                    name));
    parentLoader.setLayer(layer);
  }

  private static ClassLoader getOrCreateModuleLoader(
      Map<String, ClassLoader> loaderCache,
      ChildLayerFirstClassLoader parent,
      URL location,
      String moduleName) {
    return loaderCache.computeIfAbsent(
        moduleName, tmp -> new ChildFirstURLClassLoader(new URL[] {location}, parent));
  }

  private static void usage(String[] args) {
    System.err.printf(
        "Usage: %s <module_layer_directory> <main_class> [<other_args_for_app>]%n",
        JPMSMain.class.getSimpleName());
    throw new IllegalArgumentException(
        String.format("Too few arguments provided: %s", Arrays.toString(args)));
  }

  private JPMSMain() {}
}
