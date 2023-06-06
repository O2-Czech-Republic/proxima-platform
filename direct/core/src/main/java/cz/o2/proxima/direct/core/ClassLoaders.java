/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.core;

import cz.o2.proxima.internal.com.google.common.annotations.VisibleForTesting;
import cz.o2.proxima.internal.com.google.common.base.Preconditions;
import java.lang.module.ModuleFinder;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ClassLoaders {

  static class ChildLayerFirstClassLoader extends ClassLoader {

    private final ModuleFinder finder;

    private final List<String> moduleNames;

    private ModuleLayer layer = null;

    public ChildLayerFirstClassLoader(ModuleFinder finder, ClassLoader parent) {
      super(parent);
      this.finder = finder;
      this.moduleNames = getNamesForModules(finder).collect(Collectors.toList());
    }

    @VisibleForTesting
    Stream<String> getNamesForModules(ModuleFinder finder) {
      return finder.findAll().stream().map(r -> r.descriptor().name());
    }

    void setLayer(ModuleLayer layer) {
      Objects.requireNonNull(layer);
      Preconditions.checkState(this.layer == null);
      this.layer = layer;
    }

    private Class<?> loadClassFromModules(String name, boolean resolve) {
      for (String module : moduleNames) {
        try {
          return getModuleLoader(module).loadClassFromSelf(name, resolve);
        } catch (ClassNotFoundException ex) {
          // continue
        }
      }
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
  }

  static class ChildFirstURLClassLoader extends URLClassLoader {

    public ChildFirstURLClassLoader(URL[] urls, ChildLayerFirstClassLoader parent) {
      super(urls, parent);
    }

    @Override
    protected Class<?> findClass(String moduleName, String name) {
      return super.findClass(null, name);
    }

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

  private ClassLoaders() {}
}
