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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.AttributeWriterBase;
import cz.o2.proxima.storage.DataAccessor;
import cz.o2.proxima.storage.StorageDescriptor;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.storage.batch.BatchLogObservable;
import cz.o2.proxima.storage.commitlog.CommitLogReader;
import cz.o2.proxima.storage.randomaccess.RandomAccessReader;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.util.NamePattern;
import cz.seznam.euphoria.core.client.functional.VoidFunction;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Configuration;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Repository of all entities configured in the system.
 */
@Slf4j
public class Repository {

  /**
   * Construct default repository from the config.
   * @param config configuration to use
   * @return loaded {@link Repository}
   */
  public static Repository of(Config config) {
    return Repository.Builder.of(config).build();
  }

  /**
   * Builder for repository.
   */
  public static class Builder {

    public static Builder of(Config config) {
      return new Builder(config, false);
    }
    public static Builder ofTest(Config config) {
      return new Builder(config, true);
    }

    private final Config config;
    private VoidFunction<ExecutorService> executorFactory;
    private boolean readOnly = false;
    private boolean validate = true;
    private boolean loadFamilies = true;
    private boolean loadAccessors = true;

    private Builder(Config config, boolean test) {
      this.config = Objects.requireNonNull(config);
      this.executorFactory = () -> Executors.newCachedThreadPool(r -> {
          Thread t = new Thread(r);
          t.setName("ProximaRepositoryPool");
          t.setUncaughtExceptionHandler((thr, exc) -> {
            log.error("Error running task in thread {}", thr.getName(), exc);
          });
          return t;
        });

      if (test) {
        this.readOnly = true;
        this.validate = false;
        this.loadFamilies = false;
        this.loadAccessors = false;
      }
    }

    public Builder withReadOnly(boolean flag) {
      this.readOnly = flag;
      return this;
    }

    public Builder withValidate(boolean flag) {
      this.validate = flag;
      return this;
    }

    public Builder withLoadFamilies(boolean flag) {
      this.loadFamilies = flag;
      return this;
    }

    public Builder withLoadAccessors(boolean flag) {
      this.loadAccessors = flag;
      return this;
    }

    public Builder withExecutorFactory(
        VoidFunction<ExecutorService> executorFactory) {
      this.executorFactory = executorFactory;
      return this;
    }

    public Repository build() {
      return new Repository(
          config, readOnly, validate, loadFamilies,
          loadAccessors, executorFactory);
    }
  }


  /**
   * Application configuration.
   */
  @Getter
  private final Config config;


  /**
   * Classpath reflections scanner.
   */
  @Getter
  private final Reflections reflections;


  /**
   * When read-only flag is specified, some checks are not performed in construction.
   * This enables to use the repository inside reader applications that
   * don't have to have all the server jars on classpath.
   */
  private final boolean isReadonly;


  /**
   * Flag to indicate if we should validate the scheme with serializer.
   * Defaults to {@code true}. {@code false} can be used when
   * the classpath is not completed during constructing this object.
   * This is useful mostly inside the maven plugin.
   */
  private final boolean shouldValidate;


  /**
   * Flag to indicate we should or should not load accessor to column families.
   * The accessor is not needed mostly in the compiler.
   */
  private final boolean shouldLoadAccessors;


  /**
   * Map of all storage descriptors available.
   * Key is acceptable scheme of the descriptor.
   * This need not be synchronized because it is only written in constructor
   * and then it is read-only.
   **/
  private final Map<String, StorageDescriptor> schemeToStorage = new HashMap<>();


  /**
   * Map of all scheme serializers.
   * Key is acceptable scheme of the serializer.
   * This need not be synchronized because it is only written in constructor
   * and then it is read-only.
   */
  private final Map<String, ValueSerializerFactory> serializersMap = new HashMap<>();


  /**
   * Map of all entities specified by name.
   * This need not be synchronized because it is only written in constructor
   * and then it is read-only.
   **/
  private final Map<String, EntityDescriptor> entitiesByName = new HashMap<>();


  /**
   * Map of entities by pattern.
   * This need not be synchronized because it is only written in constructor
   * and then it is read-only.
   **/
  private final Map<NamePattern, EntityDescriptor> entitiesByPattern;


  /**
   * Map of attribute descriptor to list of families.
   * This need not be synchronized because it is only written in constructor
   * and then it is read-only.
   */
  private final Map<AttributeDescriptor<?>, Set<AttributeFamilyDescriptor>> attributeToFamily;


  /**
   * Map of transformation name to transformation descriptor.
   */
  private final Map<String, TransformationDescriptor> transformations = new HashMap<>();


  /**
   * Executor to be used for any asynchronous operations.
   */
  private final VoidFunction<ExecutorService> executorFactory;

  /**
   * Context passed to serializable data accessors.
   */
  private final Context context;

  /**
   * Construct the repository from the config with the specified read-only and
   * validation flag.
   * @param isReadonly true in client applications where you want
   * to use repository specifications to read from the datalake.
   * @param shouldValidate set to false to skip some sanity checks (not recommended)
   * @param loadFamilies should we load attribute families? This is needed
   *                     only during runtime, for maven plugin it is set to false
   * @param loadAccessors should we load accessors to column families? When not loaded
   *                      the repository will not be usable neither for reading
   *                      nor for writing (this is usable merely for code generation)
   */
  private Repository(
      Config cfg,
      boolean isReadonly,
      boolean shouldValidate,
      boolean loadFamilies,
      boolean loadAccessors,
      VoidFunction<ExecutorService> executorFactory) {

    this.config = cfg;
    this.executorFactory = executorFactory;
    this.context = new Context(executorFactory);

    this.entitiesByPattern = new HashMap<>();
    this.attributeToFamily = new HashMap<>();
    try {
      Configuration reflectionConf = ConfigurationBuilder
          .build(
              ClasspathHelper.forManifest(),
              ClasspathHelper.forClassLoader());

      this.isReadonly = isReadonly;
      this.shouldValidate = shouldValidate;
      this.shouldLoadAccessors = loadAccessors;

      reflections = new Reflections(reflectionConf);

      /* First read all storage implementations available to the repository. */
      Collection<StorageDescriptor> storages = findStorageDescriptors(reflections);
      readStorages(storages);

      /* Next read all scheme serializers. */
      Collection<ValueSerializerFactory> serializers = findSchemeSerializers(reflections);
      readSchemeSerializers(serializers);

      /* Read the config and store entitiy descriptors */
      readEntityDescriptors(cfg);

      if (loadFamilies) {

        /* Read attribute families and map them to storages by attribute. */
        readAttributeFamilies(cfg);

        if (shouldLoadAccessors) {
          /* Link attribute families for proxied attribute. */
          loadProxiedFamilies(cfg);

          /* Read transformations from one entity to another. */
          readTransformations(cfg);

          linkAttributesToWriters();
        }

      }

      if (shouldValidate) {
        /* And validate all postconditions. */
        validate();
      }

    } catch (Exception ex) {
      throw new IllegalArgumentException("Cannot read config settings", ex);
    }

  }

  /**
   * Retrieve {@link Context} that is used in all distributed operations.
   * @return the serializable context
   */
  public Context getContext() {
    return context;
  }

  @SuppressWarnings("unchecked")
  private Collection<StorageDescriptor> findStorageDescriptors(
      Reflections reflections) {

    return (Collection) findImplementingClasses(
        StorageDescriptor.class, reflections);
  }

  @SuppressWarnings("unchecked")
  private Collection<ValueSerializerFactory> findSchemeSerializers(
      Reflections reflections) {

    return findImplementingClasses(ValueSerializerFactory.class, reflections);
  }

  @SuppressWarnings("unchecked")
  private <T> Collection<T> findImplementingClasses(
      Class<T> superCls, Reflections reflections) {

    Set<Class<? extends T>> types = reflections.getSubTypesOf(superCls);
    Collection ret = (Collection) types.stream().map(c -> {
      if (!c.isAnonymousClass()) {
        try {
          return c.newInstance();
        } catch (IllegalAccessException | InstantiationException ex) {
          log.warn("Failed to instantiate class {}", c.getName(), ex);
        }
      }
      return null;
    })
    .filter(Objects::nonNull)
    .collect(Collectors.toList());
    log.info("Found {} classes implementing {}", ret.size(), superCls.getName());
    return ret;
  }

  private <T> T newInstance(String name, Class<T> cls) {
    try {
      Class<T> forName = Classpath.findClass(name, cls);
      return forName.newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      throw new IllegalArgumentException("Cannot instantiate class " + name, ex);
    }
  }

  private void readStorages(Collection<StorageDescriptor> storages) {
    storages.forEach(store ->
        store.getAcceptableSchemes().forEach(s -> {
          log.info("Adding storage descriptor {} for scheme {}://",
              store.getClass().getName(), s);
          schemeToStorage.put(s, store);
        }));
  }

  private void readSchemeSerializers(
      Collection<ValueSerializerFactory> serializers) {
    serializers.forEach(v -> {
      log.info("Added scheme serializer {} for scheme {}",
          v.getClass().getName(), v.getAcceptableScheme());
      serializersMap.put(v.getAcceptableScheme(), v);
    });
  }

  /** Read descriptors of entites from config */
  private void readEntityDescriptors(Config cfg) throws Exception {
    ConfigValue entities = cfg.root().get("entities");
    if (entities == null) {
      log.warn("Empty configuration of entities, skipping initialization");
      return;
    }
    Map<String, Object> entitiesCfg = toMap("entities", entities.unwrapped());
    for (Map.Entry<String, Object> e : entitiesCfg.entrySet()) {
      String entityName = e.getKey();
      Map<String, Object> entityAttrs = toMap(
          "entities." + entityName + ".attributes",
          toMap("entities." + entityName, e.getValue()).get("attributes"));
      EntityDescriptor.Builder entity = EntityDescriptor.newBuilder()
          .setName(entityName);

      // first regular attributes
      entityAttrs.forEach((key, value) -> {
        Map<String, Object> settings = toMap(
            "entities." + entityName + ".attributes." + key, value);
        if (settings.get("proxy") == null) {
          loadRegular(entityName, key, settings, entity);
        }
      });

      // next proxies
      entityAttrs.forEach((key, value) -> {
        Map<String, Object> settings = toMap(
            "entities." + entityName + ".attributes." + key, value);
        if (settings.get("proxy") != null) {
          loadProxy(key, settings, entity);
        }
      });

      if (!entityName.contains("*")) {
        log.info("Adding entity by fully qualified name {}", entityName);
        entitiesByName.put(entityName, entity.build());
      } else {
        log.info("Adding entity by pattern {}", entityName);
        entitiesByPattern.put(new NamePattern(entityName), entity.build());
      }

    }
  }

  @SuppressWarnings("unchecked")
  private void loadProxy(
      String attrName,
      Map<String, Object> settings,
      EntityDescriptor.Builder entityBuilder) {

    AttributeDescriptorBase target = Optional.ofNullable(settings.get("proxy"))
        .map(Object::toString)
        .map(proxy -> entityBuilder.findAttribute(proxy))
        .orElseThrow(() -> new IllegalStateException(
            "Invalid state: `proxy` should not be null"));

    final ProxyTransform transform;
    if (this.shouldLoadAccessors) {
      transform = Optional.ofNullable(settings.get("apply"))
          .map(Object::toString)
          .map(s -> newInstance(s, ProxyTransform.class))
          .orElseThrow(() -> new IllegalArgumentException("Missing required field `apply'"));
    } else {
      transform = null;
    }

    entityBuilder.addAttribute(
        AttributeDescriptor.newProxy(attrName, target, transform));
  }


  private void loadRegular(
      String entityName,
      String attrName,
      Map<String, Object> settings,
      EntityDescriptor.Builder entityBuilder) {

    try {

      Object scheme = Objects.requireNonNull(
          settings.get("scheme"),
          "Missing key entities." + entityName + ".attributes."
              + attrName + ".scheme");

      String schemeStr = scheme.toString();
      if (schemeStr.indexOf(':') == -1) {
        // if the scheme does not contain `:' the java.net.URI cannot parse it
        // we will fix this by adding `:///'
        schemeStr += ":///";
      }
      URI schemeURI = new URI(schemeStr);
      // validate that the scheme serializer doesn't throw exceptions
      // ignore the return value
      try {
        if (shouldValidate) {
          getValueSerializerFactory(schemeURI.getScheme())
              .getValueSerializer(schemeURI)
              .isValid(new byte[] { });
        }
      } catch (Exception ex) {
        throw new IllegalStateException("Cannot use serializer for URI " + schemeURI, ex);
      }
      entityBuilder.addAttribute(AttributeDescriptor.newBuilder(this)
          .setEntity(entityName)
          .setName(attrName)
          .setSchemeURI(schemeURI)
          .build());
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> toMap(String key, Object value) {
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException(
          "Key " + key + " must be object got "
          + (value != null
              ? value.getClass().getName()
              : "(null)"));
    }
    return (Map<String, Object>) value;
  }


  /**
   * Find entity descriptor based on entity name.
   * @param name name of the entity to search for
   * @return optional {@link EntityDescriptor} found by name
   */
  public Optional<EntityDescriptor> findEntity(String name) {
    EntityDescriptor byName = entitiesByName.get(name);
    if (byName != null) {
      return Optional.of(byName);
    }
    // we don't have exact match based on name, so match patterns
    return entitiesByPattern.entrySet().stream()
        .filter(e -> e.getKey().matches(name))
        .findFirst()
        .map(Map.Entry::getValue);
  }

  /**
   * Retrieve value serializer for given scheme.
   * @param scheme scheme of the {@link ValueSerializerFactory}
   * @return {@link ValueSerializerFactory} for the scheme
   */
  public ValueSerializerFactory<?> getValueSerializerFactory(String scheme) {
    ValueSerializerFactory serializer = serializersMap.get(scheme);
    if (serializer == null) {
      throw new IllegalArgumentException("Missing serializer for scheme " + scheme);
    }
    return serializer;
  }

  @SuppressWarnings("unchecked")
  private void readAttributeFamilies(Config cfg) {

    if (entitiesByName.isEmpty() && entitiesByPattern.isEmpty()) {
      // no loaded entities, no more stuff to read
      return;
    }
    Map<String, Object> entitiesCfg = toMap("attributeFamilies",
        Objects.requireNonNull(
            cfg.root().get("attributeFamilies"),
            "Missing required `attributeFamilies' settings")
        .unwrapped());

    Set<String> familyNames = new HashSet<>();

    for (Map.Entry<String, Object> e : entitiesCfg.entrySet()) {
      String name = e.getKey();
      Map<String, Object> storage = flatten(
          toMap("attributeFamilies." + name, e.getValue()));
      if (!familyNames.add(name)) {
        throw new IllegalArgumentException("Multiple attribute family names " + name);
      }

      try {
        String entity = Objects.requireNonNull(storage.get("entity")).toString();
        String filter = toString(storage.get("filter"));
        // type is one of the following:
        // * commit (stream commit log)
        // * append (append-only stream or batch)
        // * random-access (random-access read-write)
        StorageType type = StorageType.of((String) storage.get("type"));
        AccessType access = AccessType.from(
            Optional.ofNullable(storage.get("access"))
                .map(Object::toString)
                .orElse("read-only"));

        List<String> attributes = toList(
            Objects.requireNonNull(storage.get("attributes"),
                "Missing required field `attributes' in attributeFamily "
                + name));
        URI storageURI = new URI(Objects.requireNonNull(
            storage.get("storage"),
            "Missing required field `storage' in attribute family " + name)
            .toString());
        final StorageDescriptor storageDesc = isReadonly
            ? asReadOnly(schemeToStorage.get(storageURI.getScheme()))
            : schemeToStorage.get(storageURI.getScheme());
        EntityDescriptor entDesc = findEntity(entity)
            .orElseThrow(() -> new IllegalArgumentException(
                "Cannot find entity " + entity));
        if (storageDesc == null) {
          throw new IllegalArgumentException(
              "No storage for scheme "
              + storageURI.getScheme());
        }

        AttributeFamilyDescriptor.Builder family = AttributeFamilyDescriptor.newBuilder()
            .setName(name)
            .setType(type)
            .setAccess(access);

        if (shouldLoadAccessors) {
          DataAccessor accessor = storageDesc.getAccessor(
              entDesc, storageURI, storage);

          if (!isReadonly && !access.isReadonly()) {
            family.setWriter(accessor.getWriter(context)
                .orElseThrow(() -> new IllegalArgumentException(
                    "Storage " + storageDesc + " has no valid writer for family " + name
                        + " or specify the family as read-only.")));
          }
          if (access.canRandomRead()) {
            family.setRandomAccess(accessor.getRandomAccessReader(context)
                .orElseThrow(() -> new IllegalArgumentException(
                    "Storage " + storageDesc + " has no valid random access storage for family "
                        + name)));
          }
          if (access.canReadCommitLog()) {
            family.setCommitLog(accessor.getCommitLogReader(context).orElseThrow(
                () -> new IllegalArgumentException(
                    "Storage " + storageDesc
                        + " has no valid commit-log storage for family " + name)));
          }
          if (access.canCreatePartitionedView()) {
            family.setPartitionedView(accessor.getPartitionedView(context)
                .orElseThrow(() -> new IllegalArgumentException(
                    "Storage " + storageDesc + " has no valid partitioned view.")));
          }
          if (access.canReadBatchSnapshot() || access.canReadBatchUpdates()) {
            family.setBatchObservable(accessor.getBatchLogObservable(context)
                .orElseThrow(() -> new IllegalArgumentException(
                    "Storage " + storageDesc + " has no batch log observable.")));
          }
        }

        if (!filter.isEmpty() && !isReadonly) {
          if (type == StorageType.PRIMARY) {
            throw new IllegalArgumentException("Primary storage cannot have filters");
          }
          family.setFilter(newInstance(filter, StorageFilter.class));
        }

        Collection<AttributeDescriptor<?>> allAttributes = new HashSet<>();

        for (String attr : attributes) {
          // attribute descriptors affected by this settings
          final List<AttributeDescriptor<?>> attrDescs;
          if (attr.equals("*")) {
            // this means all attributes of entity
            attrDescs = (List) entDesc.getAllAttributes(true);
          } else {
            attrDescs = (List) Arrays.asList(entDesc.findAttribute(attr, true)
                .orElseThrow(
                    () -> new IllegalArgumentException("Cannot find attribute " + attr)));
          }
          allAttributes.addAll(attrDescs);
        }

        allAttributes.forEach(family::addAttribute);
        final AttributeFamilyDescriptor familyBuilt = family.build();
        allAttributes.forEach(a -> {
          Set<AttributeFamilyDescriptor> families = attributeToFamily
              .computeIfAbsent(a, k -> new HashSet<>());
          if (!families.add(familyBuilt)) {
            throw new IllegalArgumentException(
                "Attribute family named "
                + a.getName() + " already exists");
          }
          log.debug(
              "Added family {} for entity {} of type {} and access {}",
              familyBuilt, entDesc, familyBuilt.getType(), familyBuilt.getAccess());
        });
      } catch (URISyntaxException ex) {
        throw new IllegalArgumentException("Cannot parse input URI " + storage.get("storage"), ex);
      }

    }

  }

  private void linkAttributesToWriters() {
    // iterate over all attribute families and setup appropriate (commit) writers
    // for all attributes
    attributeToFamily.forEach((key, value) -> {
      Optional<AttributeWriterBase> writer = value
          .stream()
          .filter(af -> af.getType() == StorageType.PRIMARY)
          .filter(af -> !af.getAccess().isReadonly())
          .filter(af -> af.getWriter().isPresent())
          .findAny()
          .map(af -> af.getWriter()
              .orElseThrow(() -> new NoSuchElementException("Writer can not be empty")));

      if (writer.isPresent()) {
        ((AttributeDescriptorBase<?>) key).setWriter(writer.get().online());
      } else {
        log.info(
            "No writer found for attribute {}, continuing, but assuming "
                + "the attribute is read-only. Any attempt to write it will fail.",
            key);
      }
    });
  }

  private void loadProxiedFamilies(Config cfg) {
    getAllEntities()
        .flatMap(e -> e.getAllAttributes(true).stream())
        .filter(a -> ((AttributeDescriptorBase<?>) a).isProxy())
        .forEach(a -> {
          AttributeProxyDescriptorImpl<?> p = (AttributeProxyDescriptorImpl<?>) a;
          AttributeDescriptorBase<?> target = p.getTarget();
          attributeToFamily.put(p, getFamiliesForAttribute(target)
              .stream()
              .map(af -> new AttributeFamilyProxyDescriptor(p, af))
              .collect(Collectors.toSet()));
        });
  }

  private void readTransformations(Config cfg) {

    if (entitiesByName.isEmpty() && entitiesByPattern.isEmpty()) {
      // no loaded entities, no more stuff to read
      return;
    }
    Map<String, Object> transformations = Optional.ofNullable(
        cfg.root().get("transformations"))
        .map(v -> toMap("transformations", v.unwrapped()))
        .orElse(null);

    if (transformations == null) {
      log.info("Skipping empty transformations configuration.");
      return;
    }

    transformations.forEach((k, v) -> {
      Map<String, Object> transformation = toMap(k, v);
      EntityDescriptor entity = findEntity(readStr("entity", transformation, k))
          .orElseThrow(() -> new IllegalArgumentException(
              String.format("Entity `%s` doesn't exist",
                  transformation.get("entity"))));

      Transformation t = newInstance(
          readStr("using", transformation, k), Transformation.class);

      List<AttributeDescriptor<?>> attrs = readList("attributes", transformation, k)
          .stream()
          .map(a -> entity.findAttribute(a, true).orElseThrow(
              () -> new IllegalArgumentException(
                  String.format("Missing attribute `%s` in `%s`",
                      a, entity))))
          .collect(Collectors.toList());

      TransformationDescriptor.Builder desc = TransformationDescriptor.newBuilder()
          .addAttributes(attrs)
          .setEntity(entity)
          .setTransformation(t);

      Optional
          .ofNullable(transformation.get("filter"))
          .map(Object::toString)
          .map(s -> newInstance(s, StorageFilter.class))
          .ifPresent(desc::setFilter);

      this.transformations.put(k, desc.build());

    });

    this.transformations.forEach((k, v) -> v.getTransformation().setup(this));

  }

  private static String readStr(String key, Map<String, Object> map, String name) {
    return Optional.ofNullable(map.get(key))
          .map(Object::toString)
          .orElseThrow(
              () -> new IllegalArgumentException(
                  String.format("Missing required field `%s` in `%s`", key, name)));
  }

  @SuppressWarnings("unchecked")
  private static List<String> readList(
      String key, Map<String, Object> map, String name) {

    return Optional.ofNullable(map.get(key))
        .map(v -> {
          if (v instanceof List) return (List<Object>) v;
          throw new IllegalArgumentException(
              String.format("Key `%s` in `%s` must be list", key, name));
        })
        .map(l -> l.stream().map(Object::toString).collect(Collectors.toList()))
        .orElseThrow(() -> new IllegalArgumentException(
            String.format("Missing required field `%s` in `%s", key, name)));
  }




  @SuppressWarnings("unchecked")
  private List<String> toList(Object in) {
    if (in instanceof List) {
      return (List) ((List) in).stream()
          .map(Object::toString).collect(Collectors.toList());
    }
    return Collections.singletonList(in.toString());
  }

  private static String toString(Object what) {
    return what == null ? "" : what.toString();
  }

  // check validity of the settings
  private void validate() {
    // validate that each attribute belongs to at least one attribute family

    Stream.concat(
            entitiesByName.values().stream(),
            entitiesByPattern.values().stream())
        .flatMap(d -> d.getAllAttributes(true).stream())
        .filter(a -> !((AttributeDescriptorBase<?>) a).isProxy())
        .filter(a -> {
          Set<AttributeFamilyDescriptor> families = attributeToFamily.get(a);
          return families == null || families.isEmpty();
        })
        .findAny()
        .ifPresent(a -> {
            throw new IllegalArgumentException("Attribute " + a.getName()
                + " of entity " + a.getEntity() + " has no storage");
        });

  }

  /**
   * Retrieve storage descriptor by scheme.
   * @param scheme storage scheme to look for
   * @return {@link StorageDescriptor} for the specified scheme
   */
  public StorageDescriptor getStorageDescriptor(String scheme) {
    StorageDescriptor desc = this.schemeToStorage.get(scheme);
    if (desc == null) {
      throw new IllegalArgumentException("No storage for scheme " + scheme);
    }
    return desc;
  }


  /**
   * List all unique attribute families.
   * @return all families specified in this repository
   */
  public Stream<AttributeFamilyDescriptor> getAllFamilies() {
    return attributeToFamily.values().stream()
        .flatMap(Collection::stream).distinct();
  }

  /**
   * Retrieve list of attribute families for attribute.
   * @param attr attribute descriptor
   * @return all families of given attribute
   */
  public Set<AttributeFamilyDescriptor> getFamiliesForAttribute(
      AttributeDescriptor<?> attr) {

    return Objects.requireNonNull(
        attributeToFamily.get(attr),
        "Cannot find any family for attribute " + attr);
  }

  /**
   * Retrieve stream of all entities.
   * @return {@link Stream} of all entities specified in this repository
   */
  public Stream<EntityDescriptor> getAllEntities() {
    return Stream.concat(
            entitiesByName.values().stream(),
            entitiesByPattern.values().stream());
  }

  /**
   * Retrieve all transformers.
   * @return all transformations by name
   */
  public Map<String, TransformationDescriptor> getTransformations() {
    return Collections.unmodifiableMap(transformations);
  }

  /**
   * Check if this repostiory is empty.
   * @return {@code true} if this repository is empty
   */
  public boolean isEmpty() {
    return this.entitiesByName.isEmpty() && entitiesByPattern.isEmpty();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> flatten(Map<String, Object> map) {
    Map<String, Object> ret = new HashMap<>();
    map.forEach((key, value) -> {
      if (value instanceof Map) {
        final Map<String, Object> flattened = flatten((Map) value);
        flattened.forEach((key1, value1) -> ret.put(key + "." + key1, value1));
      } else {
        ret.put(key, value);
      }
    });
    return ret;
  }

  /* Wrap given storage descriptor to read-only version. */
  private <T extends AttributeWriterBase> StorageDescriptor asReadOnly(
      StorageDescriptor wrap) {

    return new StorageDescriptor(wrap.getAcceptableSchemes()) {

      @Override
      public DataAccessor getAccessor(
          EntityDescriptor entityDesc, URI uri, Map<String, Object> cfg) {

        DataAccessor wrapped = wrap.getAccessor(entityDesc, uri, cfg);

        return new DataAccessor() {

          @Override
          public Optional<CommitLogReader> getCommitLogReader(Context context) {
            return wrapped.getCommitLogReader(context);
          }

          @Override
          public Optional<RandomAccessReader> getRandomAccessReader(Context context) {
            return wrapped.getRandomAccessReader(context);
          }

          @Override
          public Optional<BatchLogObservable> getBatchLogObservable(Context context) {
            return wrapped.getBatchLogObservable(context);
          }

        };
      }

    };
  }

}
