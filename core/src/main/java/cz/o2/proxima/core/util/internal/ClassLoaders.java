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
package cz.o2.proxima.core.util.internal;

import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.io.IOException;
import java.lang.module.ModuleFinder;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClassLoaders {

  public static class ChildLayerFirstClassLoader extends ClassLoader {

    private final List<String> moduleNames;

    @Getter private ModuleLayer layer = null;

    public ChildLayerFirstClassLoader(ModuleFinder finder, ClassLoader parent) {
      super(parent);
      this.moduleNames = getNamesForModules(finder).collect(Collectors.toList());
      log.info("Created {} with modules {}", getClass().getSimpleName(), moduleNames);
    }

    @VisibleForTesting
    Stream<String> getNamesForModules(ModuleFinder finder) {
      return finder.findAll().stream().map(r -> r.descriptor().name());
    }

    public void setLayer(ModuleLayer layer) {
      Objects.requireNonNull(layer);
      Preconditions.checkState(this.layer == null);
      this.layer = layer;
    }

    private Class<?> loadClassFromModules(String name, boolean resolve) {
      for (String module : moduleNames) {
        try {
          log.trace("Trying to load class {} from module {}", name, module);
          return getModuleLoader(module).loadClassFromSelf(name, resolve);
        } catch (ClassNotFoundException ex) {
          // continue
          log.trace("Class {} not found module {}", name, module);
        }
      }
      log.trace("No module found for class {}", name);
      return null;
    }

    @VisibleForTesting
    ChildFirstURLClassLoader getModuleLoader(String module) {
      return (ChildFirstURLClassLoader) layer.findLoader(module);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      Preconditions.checkState(this.layer != null);
      Class<?> loadedClass = loadClassFromModules(name, resolve);
      if (loadedClass == null) {
        loadedClass = super.loadClass(name, resolve);
      }

      if (resolve) {
        resolveClass(loadedClass);
      }
      return loadedClass;
    }

    @Override
    protected URL findResource(String moduleName, String name) {
      return Objects.requireNonNull(getModuleLoader(moduleName)).findResource(name);
    }

    @Override
    protected URL findResource(String name) {
      for (String module : moduleNames) {
        URL resource = getModuleLoader(module).findResource(name);
        if (resource != null) {
          return resource;
        }
      }
      return null;
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
      Set<URL> urls = new HashSet<>();
      for (String module : moduleNames) {
        urls.addAll(Collections.list(getModuleLoader(module).findResources(name)));
      }
      return Collections.enumeration(urls);
    }
  }

  public static class ChildFirstURLClassLoader extends URLClassLoader {

    public ChildFirstURLClassLoader(URL[] urls, ChildLayerFirstClassLoader parent) {
      super(urls, parent);
    }

    @Override
    protected Class<?> findClass(String moduleName, String name) {
      return super.findClass(null, name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      return loadClass(name, resolve, true);
    }

    public Class<?> loadClassFromSelf(String name, boolean resolve) throws ClassNotFoundException {
      return loadClass(name, resolve, false);
    }

    private Class<?> loadClass(String name, boolean resolve, boolean callParent)
        throws ClassNotFoundException {

      Class<?> loadedClass = findLoadedClass(name);
      if (loadedClass == null) {
        try {
          loadedClass = findClass(name);
        } catch (ClassNotFoundException ex) {
          if (callParent) {
            loadedClass = super.loadClass(name, resolve);
          } else {
            throw new ClassNotFoundException(name);
          }
        }
      }
      if (resolve) {
        resolveClass(loadedClass);
      }
      return loadedClass;
    }
  }

  public static class ContextLoaderFence implements AutoCloseable {

    private final ClassLoader old = Thread.currentThread().getContextClassLoader();

    public ContextLoaderFence(ClassLoader loader) {
      Thread.currentThread().setContextClassLoader(loader);
    }

    @Override
    public void close() {
      Thread.currentThread().setContextClassLoader(old);
    }
  }

  private ClassLoaders() {}
}
