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
package cz.o2.proxima.repository;

import static cz.o2.proxima.repository.ConfigConstants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import cz.o2.proxima.functional.BiFunction;
import cz.o2.proxima.functional.UnaryFunction;
import cz.o2.proxima.repository.RepositoryFactory.LocalInstance;
import cz.o2.proxima.repository.RepositoryFactory.VersionedCaching;
import cz.o2.proxima.scheme.ValueSerializerFactory;
import cz.o2.proxima.storage.AccessType;
import cz.o2.proxima.storage.StorageFilter;
import cz.o2.proxima.storage.StorageType;
import cz.o2.proxima.transform.DataOperatorAware;
import cz.o2.proxima.transform.ElementWiseProxyTransform;
import cz.o2.proxima.transform.ElementWiseProxyTransform.ProxySetupContext;
import cz.o2.proxima.transform.ProxyTransform;
import cz.o2.proxima.transform.RenameTransformation;
import cz.o2.proxima.transform.Transformation;
import cz.o2.proxima.util.CamelCase;
import cz.o2.proxima.util.Classpath;
import cz.o2.proxima.util.Pair;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/** Repository of all entities configured in the system. */
@Slf4j
// BUG in error prone
@SuppressWarnings("InconsistentCapitalization")
public final class ConfigRepository extends Repository {

  private static final long serialVersionUID = 1L;

  private static final Pattern ENTITY_NAME_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
  private static final Pattern ATTRIBUTE_NAME_PATTERN =
      Pattern.compile("[a-zA-Z_][a-zA-Z0-9_\\-$]*(\\.\\*)?");

  private static Config cachedConfigConstructed;

  /**
   * Create config from /{@link Config}.
   *
   * @param config the config to create {@link Repository} from
   * @return new {@link Repository}
   */
  public static Repository of(Config config) {
    return Builder.of(config).build();
  }

  /**
   * Construct default repository from the config.
   *
   * @param config configuration to use
   * @param validates which validations to perform
   * @return constructed repository
   */
  public static Repository ofTest(Config config, Validate... validates) {
    Builder builder = Builder.of(config).withCachingEnabled(false);
    if (validates.length == 0) {
      builder = builder.withValidateFlag(Validate.defaultTesting());
    } else {
      builder = builder.withValidate(validates);
    }
    return builder.build();
  }

  /** Builder for the repository. */
  public static class Builder {

    /**
     * Create new {@link Repository} from {@link Config}.
     *
     * @param config config to create {@link Repository from}
     * @return new builder of Repository
     */
    public static Builder of(Config config) {
      return new Builder(config);
    }

    /**
     * Create a test version of repository.
     *
     * @param config config to use
     * @return new Builder of test Repository
     */
    public static Builder ofTest(Config config) {
      return new Builder(config).withCachingEnabled(false);
    }

    private final Config config;
    private boolean cachingEnabled = true;
    private boolean readOnly = false;
    private int validate;
    private boolean loadFamilies = true;
    private boolean loadClasses = true;
    private @Nullable RepositoryFactory factory = null;

    private Builder(Config config) {
      this.config = Objects.requireNonNull(config);
      this.validate = getDefaultValidationFlags(config);
    }

    private int getDefaultValidationFlags(Config config) {
      if (config.hasPath(VALIDATIONS)) {
        return config
            .getStringList(VALIDATIONS)
            .stream()
            .map(Validate::valueOf)
            .reduce(0, (flag, v) -> flag | v.getFlag(), (a, b) -> a | b);
      }
      return Validate.ALL.getFlag();
    }

    public Builder withCachingEnabled(boolean flag) {
      this.cachingEnabled = flag;
      return this;
    }

    public Builder withReadOnly(boolean flag) {
      this.readOnly = flag;
      return this;
    }

    public Builder withValidate(Validate... values) {
      this.validate = Arrays.stream(values).map(Validate::getFlag).reduce(0, (a, b) -> a | b);
      return this;
    }

    public Builder withValidateFlag(int flag) {
      this.validate = flag;
      return this;
    }

    public Builder withLoadFamilies(boolean flag) {
      this.loadFamilies = flag;
      return this;
    }

    public Builder withLoadClasses(boolean flag) {
      this.loadClasses = flag;
      return this;
    }

    public ConfigRepository build() {
      ConfigRepository repo =
          new ConfigRepository(
              config, cachingEnabled, readOnly, validate, loadFamilies, loadClasses);
      RepositoryFactory factory =
          cachingEnabled
              ? RepositoryFactory.caching(RepositoryFactory.compressed(config), repo)
              : RepositoryFactory.local(repo, asFactory());
      return repo.withFactory(factory);
    }

    private RepositoryFactory asFactory() {
      final Config config = this.config;
      final boolean cachingEnabled = this.cachingEnabled;
      final boolean loadClasses = this.loadClasses;
      final boolean loadFamilies = this.loadFamilies;
      final boolean readOnly = this.readOnly;
      final int validate = this.validate;
      return () ->
          new Builder(config)
              .withCachingEnabled(cachingEnabled)
              .withLoadClasses(loadClasses)
              .withLoadFamilies(loadFamilies)
              .withReadOnly(readOnly)
              .withValidateFlag(validate)
              .build();
    }
  }

  /** Parsed descriptor of replication. */
  @Value
  private static class Replication {
    boolean readOnly;
    Map<String, Object> targets;
    Map<String, Object> source;
    Map<String, Object> via;
    EntityDescriptorImpl entity;
    Collection<String> attrs;
    AttributeFamilyDescriptor family;
    boolean readNonReplicated;
  }

  static void dropCached() {
    cachedConfigConstructed = null;
  }

  /** Application configuration. */
  @Getter private Config config;

  /** Test repository disables any caching. */
  @Getter private final boolean enableCaching;

  /**
   * When read-only flag is specified, some checks are not performed in construction. This enables
   * to use the repository inside reader applications that don't have to have all the server jars on
   * classpath.
   */
  @Getter private final boolean readonly;

  /**
   * Flags to indicate if we should validate perform various validations. Defaults to all
   * validations various validations can be switched off when appropriate.
   */
  private final int validateFlags;

  /**
   * Flag to indicate if we should actually load classes specified in config. This is usually set to
   * {@code true}, but in case of compiler using the repository, this might be handy to set to
   * {@code false}, because the actual classes might not be available yet at the time of the code
   * generation.
   */
  private final boolean loadClasses;

  /**
   * Map of all scheme serializers. Key is acceptable scheme of the serializer. This need not be
   * synchronized because it is only written in constructor and then it is read-only.
   */
  private final Map<String, ValueSerializerFactory> serializersMap = new HashMap<>();

  /**
   * Map of all entities specified by name. This need not be synchronized because it is only written
   * in constructor and then it is read-only.
   */
  private final Map<String, EntityDescriptor> entitiesByName = new HashMap<>();

  /**
   * Map of attribute descriptor to list of families. This need not be synchronized because it is
   * only written in constructor and then it is read-only.
   */
  private final Map<AttributeDescriptor<?>, Set<AttributeFamilyDescriptor>> attributeToFamily =
      new HashMap<>();

  /** All families that have been loaded in configuration. */
  private final Map<String, AttributeFamilyDescriptor> allCreatedFamilies = new HashMap<>();

  /** Map of transformation name to transformation descriptor. */
  private final Map<String, TransformationDescriptor> transformations = new HashMap<>();

  /** Set of operators created by this repository. */
  private final Set<DataOperator> operators = Collections.synchronizedSet(new HashSet<>());

  /**
   * Construct the repository from the config with the specified read-only and validation flag.
   *
   * @param config the config to use
   * @param cachingEnabled can we cache the Repository per JVM
   * @param isReadonly true in client applications where you want to use repository specifications
   *     to read from the datalake.
   * @param validateFlags validation {@link cz.o2.proxima.repository.Repository.Validate} flags
   * @param loadFamilies should we load attribute families? This is needed only during runtime, for
   *     maven plugin it is set to false
   */
  private ConfigRepository(
      Config config,
      boolean cachingEnabled,
      boolean isReadonly,
      int validateFlags,
      boolean loadFamilies,
      boolean loadClasses) {

    this.enableCaching = cachingEnabled;
    this.config = config;
    this.readonly = isReadonly;
    this.validateFlags = validateFlags;
    this.loadClasses = loadClasses;
    try {

      // read all scheme serializers.
      readSchemeSerializers(ServiceLoader.load(ValueSerializerFactory.class));

      reloadConfig(loadFamilies, this.config);

    } catch (Exception ex) {
      throw new IllegalArgumentException("Cannot read config settings", ex);
    }

    if (cachingEnabled) {
      if (cachedConfigConstructed != null) {
        log.warn(
            "Multiple constructors of production {} detected. This is generally not supported "
                + "and might result in non-expected behavior. Please consider constructing "
                + "the {} only once.",
            getClass().getSimpleName(),
            Repository.class.getSimpleName());
        if (!cachedConfigConstructed.equals(this.config)) {
          throw new IllegalStateException(
              String.format(
                  "Multiple different instances of %s created!", getClass().getSimpleName()));
        }
      }
      cachedConfigConstructed = this.config;
    }
  }

  /** @return this */
  ConfigRepository withFactory(RepositoryFactory factory) {
    this.factory = factory;
    return this;
  }

  public final void reloadConfig(boolean loadFamilies, Config conf) {

    this.config = conf;
    this.attributeToFamily.clear();
    this.entitiesByName.clear();
    this.transformations.clear();

    // Read the config and store entity descriptors
    readEntityDescriptors(config);

    if (loadFamilies) {
      // Read attribute families and map them to storages by attribute. */
      readAttributeFamilies(config);
      // Link attribute families for proxied attribute (non replicated)
      loadProxiedFamilies();
      // modify entites based on replications
      Map<String, Replication> replications = parseReplications(config);
      readEntityReplications(replications);
      // Create replication families
      createReplicationFamilies(replications);
      // Link attribute families for proxied attribute (replicated)
      loadProxiedFamilies(true);
      // Read transformations from one entity to another.
      readTransformations(config);
      // call setup() on all transforms
      setupTransforms();
    }

    if (isShouldValidate(Validate.FAMILIES)) {
      // Sanity checks.
      validate();
    }

    operators.forEach(DataOperator::reload);
  }

  private void setupTransforms() {
    getAllEntities()
        .flatMap(e -> e.getAllAttributes().stream())
        .filter(AttributeDescriptor::isProxy)
        .map(AttributeDescriptor::asProxy)
        .distinct()
        .forEach(
            a -> {
              if (a.isAsymmetric()) {
                setupTransform(
                    getEntity(a.getEntity()),
                    a,
                    a.getReadTarget(),
                    a.getReadTransform(),
                    true,
                    false);
                setupTransform(
                    getEntity(a.getEntity()),
                    a,
                    a.getWriteTarget(),
                    a.getWriteTransform(),
                    false,
                    true);
              } else {
                setupTransform(
                    getEntity(a.getEntity()),
                    a,
                    a.getReadTarget(),
                    a.getReadTransform(),
                    true,
                    true);
              }
            });
  }

  private void readSchemeSerializers(Iterable<ValueSerializerFactory> serializers) {
    serializers.forEach(
        v -> {
          if (!serializersMap.containsKey(v.getAcceptableScheme())) {
            log.info(
                "Added scheme serializer {} for scheme {}",
                v.getClass().getName(),
                v.getAcceptableScheme());
            serializersMap.put(v.getAcceptableScheme(), v);
          }
        });
  }

  /** Read descriptors of entities from config. */
  private void readEntityDescriptors(Config cfg) {

    ConfigValue entities = cfg.root().get(ENTITIES);
    if (entities == null) {
      log.warn("Empty configuration of entities, skipping initialization");
      return;
    }

    Map<String, Object> entitiesCfg = toMap(ENTITIES, entities.unwrapped());
    List<Pair<String, String>> clonedEntities = new ArrayList<>();
    for (Map.Entry<String, Object> e : entitiesCfg.entrySet()) {
      String entityName = validateEntityName(e.getKey());
      Map<String, Object> cfgMap = toMap("entities." + entityName, e.getValue());
      Object attributes = cfgMap.get(ATTRIBUTES);
      final EntityDescriptor entity;
      if (attributes != null) {
        if (!entitiesByName.containsKey(entityName)) {
          entity = loadEntityWithAttributes(entityName, attributes);
          log.info("Adding entity {}", entityName);
          entitiesByName.put(entityName, entity);
        }
      } else if (cfgMap.get("from") != null) {
        String fromName = cfgMap.get("from").toString();
        clonedEntities.add(Pair.of(entityName, fromName));
      } else {
        throw new IllegalArgumentException(
            "Invalid entity specification. Entity " + entityName + " has no attributes");
      }
    }

    for (Pair<String, String> p : clonedEntities) {
      String entityName = p.getFirst();
      String fromName = p.getSecond();
      EntityDescriptor from = findEntityRequired(fromName);
      EntityDescriptor entity = loadEntityFromExisting(entityName, from);
      log.info("Adding entity {} as clone of {}", entityName, fromName);
      entitiesByName.put(entityName, entity);
    }
  }

  @VisibleForTesting
  static String validateEntityName(String name) {
    return validateName(ENTITY_NAME_PATTERN, "Entity", name);
  }

  private static String validateAttributeName(String entity, String attr) {
    return validateName(
        ATTRIBUTE_NAME_PATTERN, String.format("Entity %s's attribute", entity), attr);
  }

  private static String validateName(Pattern pattern, String what, String name) {
    Preconditions.checkArgument(
        pattern.matcher(name).matches(),
        "%s [%s] contains invalid characters. Valid are patterns %s.",
        what,
        name,
        pattern.pattern());
    return name;
  }

  private Map<String, Replication> parseReplications(Config config) {
    if (!config.hasPath(REPLICATIONS)) {
      return Collections.emptyMap();
    }
    ConfigObject replications = config.getObject(REPLICATIONS);
    Map<String, Replication> ret = new HashMap<>();
    boolean disabledDefault =
        Optional.ofNullable(replications.get(DISABLED))
            .map(ConfigValue::unwrapped)
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(false);
    boolean readOnlyDefault =
        Optional.ofNullable(replications.get(READ_ONLY))
            .map(ConfigValue::unwrapped)
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(false);
    boolean readLocalDefault = getReadNonReplicated(replications, "GLOBAL", false);
    replications
        .entrySet()
        .stream()
        .forEach(
            e -> {
              Map<String, Object> replConf = toMap(e.getKey(), e.getValue().unwrapped(), false);
              if (replConf != null) {
                Replication parsed =
                    parseSingleReplication(
                        e.getKey(), replConf, disabledDefault, readOnlyDefault, readLocalDefault);
                if (parsed != null) {
                  ret.put(e.getKey(), parsed);
                }
              }
            });
    return ret;
  }

  private Replication parseSingleReplication(
      String replicationName,
      Map<String, Object> replConf,
      boolean disabledDefault,
      boolean readOnlyDefault,
      boolean readLocalDefault) {

    boolean disabled =
        Optional.ofNullable(replConf.get(DISABLED))
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(disabledDefault);
    if (disabled) {
      return null;
    }
    boolean readOnly =
        Optional.ofNullable(replConf.get(READ_ONLY))
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(readOnlyDefault);

    Map<String, Object> targets =
        Optional.ofNullable(replConf.get(TARGETS))
            .map(t -> toMap(TARGETS, t))
            .orElse(Collections.emptyMap());
    Map<String, Object> source =
        Optional.ofNullable(replConf.get(SOURCE))
            .map(s -> toMap(SOURCE, s))
            .orElse(Collections.emptyMap());
    Map<String, Object> via = toMap(VIA, replConf.get(VIA));
    EntityDescriptorImpl entity =
        Optional.ofNullable(replConf.get(ENTITY))
            .map(ent -> findEntityRequired(ent.toString()))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Missing required field `%s'", ENTITY)));
    Set<AttributeDescriptor<?>> attrs =
        Optional.ofNullable(replConf.get(ATTRIBUTES))
            .map(
                o ->
                    toList(o)
                        .stream()
                        .flatMap(a -> searchAttributesMatching(a, entity, false, true).stream())
                        .collect(Collectors.toSet()))
            .orElse(Collections.emptySet());
    Set<AttributeFamilyDescriptor> families =
        attrs
            .stream()
            .flatMap(
                a ->
                    getFamiliesForAttribute(a)
                        .stream()
                        .filter(af -> af.getType() == StorageType.PRIMARY))
            .collect(Collectors.toSet());

    if (families.size() != 1) {
      throw new IllegalArgumentException(
          "Each replication has to work on exactly single family. "
              + "Got "
              + families
              + " for "
              + replicationName
              + " with attributes "
              + attrs);
    }

    AttributeFamilyDescriptor sourceFamily = Iterables.getOnlyElement(families);
    List<String> attrNames =
        attrs.stream().map(AttributeDescriptor::getName).collect(Collectors.toList());

    boolean readNonReplicated = getReadNonReplicated(replConf, replicationName, readLocalDefault);
    return new Replication(
        readOnly, targets, source, via, entity, attrNames, sourceFamily, readNonReplicated);
  }

  private boolean getReadNonReplicated(
      Map<String, ?> conf, String replicationName, boolean defVal) {

    return Optional.ofNullable(conf.get(READ))
        .map(o -> o instanceof ConfigValue ? ((ConfigValue) o).unwrapped() : o)
        .map(Object::toString)
        .map(
            s -> {
              if (s.equals(LOCAL) || s.equals(ALL)) {
                return s;
              }
              throw new IllegalArgumentException(
                  String.format(
                      "`%s' parameter of %s must be either `%s' or `%s'",
                      READ, replicationName, LOCAL, ALL));
            })
        .map(s -> s.equals(LOCAL))
        .orElse(defVal);
  }

  @SuppressWarnings("unchecked")
  private void readEntityReplications(Map<String, Replication> replications) {
    for (Map.Entry<String, Replication> e : replications.entrySet()) {
      Replication repl = e.getValue();
      boolean readOnly = repl.isReadOnly();
      EntityDescriptorImpl entity = repl.getEntity();
      Collection<String> attrNames = repl.getAttrs();
      List<AttributeDescriptorBase<?>> attrs =
          attrNames
              .stream()
              .map(a -> findAttributeRequired(entity, a))
              .collect(Collectors.toList());
      Collection<String> targets = repl.getTargets().keySet();
      attrs.forEach(
          a -> {
            String writeName = resolveProxyTarget(entity, a.getName(), false).getName();
            String readName = resolveProxyTarget(entity, a.getName(), true).getName();
            if (!readOnly) {
              for (String target : targets) {
                String name = toReplicationTargetName(e.getKey(), target, a.getName());
                replaceAttribute(
                    entity,
                    AttributeDescriptor.newBuilder(this)
                        .setEntity(entity.getName())
                        .setSchemeUri(a.getSchemeUri())
                        .setName(name)
                        .setReplica(true)
                        .build());
              }
              replaceAttribute(
                  entity,
                  AttributeDescriptor.newBuilder(this)
                      .setEntity(entity.getName())
                      .setSchemeUri(a.getSchemeUri())
                      .setName(toReplicationWriteName(e.getKey(), writeName))
                      .setReplica(true)
                      .build());
            }
            if (!repl.isReadNonReplicated() || !repl.getSource().isEmpty()) {
              replaceAttribute(
                  entity,
                  AttributeDescriptor.newBuilder(this)
                      .setEntity(entity.getName())
                      .setSchemeUri(a.getSchemeUri())
                      .setName(toReplicationProxyName(e.getKey(), readName))
                      .setReplica(true)
                      .build());
            }

            if (!repl.getSource().isEmpty()) {
              replaceAttribute(
                  entity,
                  AttributeDescriptor.newBuilder(this)
                      .setEntity(entity.getName())
                      .setSchemeUri(a.getSchemeUri())
                      .setName(toReplicationReadName(e.getKey(), a.getName()))
                      .setReplica(true)
                      .build());
            }
          });
      log.info("Loaded replication {}", e.getKey());
    }
  }

  private void replaceAttribute(EntityDescriptorImpl entity, AttributeDescriptor<?> attr) {

    Optional<AttributeDescriptor<?>> replaced = entity.replaceAttribute(attr);
    if (replaced.isPresent()) {
      // the attribute has changed
      // change existing families appropriately
      getFamiliesForAttribute(replaced.get(), false)
          .stream()
          .map(
              af -> {
                AttributeFamilyDescriptor.Builder builder = af.toBuilder().clearAttributes();
                af.getAttributes()
                    .stream()
                    .map(a -> a.equals(attr) ? attr : a)
                    .forEach(builder::addAttribute);
                return Pair.of(af, builder.build());
              })
          .collect(Collectors.toList())
          .forEach(
              p -> {
                removeFamily(p.getFirst());
                insertFamily(p.getSecond(), false);
              });

      // and change transformations
      getTransformations()
          .entrySet()
          .stream()
          .filter(e -> e.getValue().getAttributes().contains(attr))
          .collect(Collectors.toList())
          .forEach(e -> e.getValue().replaceAttribute(attr));
    }
  }

  private String toReplicationTargetName(String replicationName, String target, String attr) {

    return CamelCase.apply(String.format("_%s_%s$%s", replicationName, target, attr), false);
  }

  private static String toReplicationProxyName(String replicationName, String attr) {

    return CamelCase.apply(String.format("_%s_replicated$%s", replicationName, attr), false);
  }

  private static String toReplicationReadName(String replicationName, String attr) {

    return CamelCase.apply(String.format("_%s_read$%s", replicationName, attr), false);
  }

  private static String toReplicationWriteName(String replicationName, String attr) {

    return CamelCase.apply(String.format("_%s_write$%s", replicationName, attr), false);
  }

  private EntityDescriptor loadEntityWithAttributes(String entityName, Object attributes) {

    Map<String, Object> entityAttrs =
        toMap(ENTITIES + "." + entityName + "." + ATTRIBUTES, attributes);
    EntityDescriptor.Builder entity = EntityDescriptor.newBuilder().setName(entityName);

    // first regular attributes
    entityAttrs.forEach(
        (key, value) -> {
          key = validateAttributeName(entityName, key);
          Map<String, Object> settings =
              toMap(ENTITIES + "." + entityName + "." + ATTRIBUTES + "." + key, value);
          if (settings.get(SCHEME) != null) {
            loadRegular(entityName, key, settings, entity);
          }
        });

    // next proxies
    entityAttrs.forEach(
        (key, value) -> {
          key = validateAttributeName(entityName, key);
          Map<String, Object> settings =
              toMap(ENTITIES + "." + entityName + "." + ATTRIBUTES + "." + key, value);
          if (settings.get(PROXY) != null) {
            loadProxyAttribute(key, settings, entity);
          }
        });
    return entity.build();
  }

  private EntityDescriptor loadEntityFromExisting(String entityName, EntityDescriptor from) {

    EntityDescriptor.Builder builder = EntityDescriptor.newBuilder().setName(entityName);
    from.getAllAttributes()
        .forEach(attr -> builder.addAttribute(attr.toBuilder(this).setEntity(entityName).build()));
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private void loadProxyAttribute(
      String attrName, Map<String, Object> settings, EntityDescriptor.Builder entityBuilder) {

    if (settings.get(PROXY) instanceof Map) {
      Map<String, Object> proxyMap = (Map) settings.get(PROXY);
      // copy scheme from upper to inner map, if needed
      proxyMap.computeIfAbsent(SCHEME, settings::get);
      addProxyAttributeAsymmetric(attrName, proxyMap, entityBuilder);
    } else {
      final AttributeDescriptor<Object> writeTarget;
      final ProxyTransform readTransform;
      final ProxyTransform writeTransform;
      final AttributeDescriptor<Object> readTarget =
          writeTarget =
              Optional.ofNullable(settings.get(PROXY))
                  .map(Object::toString)
                  .map(entityBuilder::getAttribute)
                  .map(a -> (AttributeDescriptor<Object>) a)
                  .orElseThrow(
                      () -> new IllegalStateException("Invalid state: `proxy` must not be null"));
      if (loadClasses) {
        readTransform = writeTransform = getProxyTransform(settings);
      } else {
        readTransform = writeTransform = null;
      }
      URI schemeURI = readProxySchemeOptional(attrName, settings, writeTarget, readTarget);
      entityBuilder.addAttribute(
          AttributeDescriptor.newProxy(
              attrName,
              readTarget,
              readTransform,
              writeTarget,
              writeTransform,
              schemeURI,
              requireValueSerializerFactory(schemeURI).getValueSerializer(schemeURI)));
    }
  }

  private void setupTransform(
      EntityDescriptor entity,
      AttributeDescriptor<?> proxyAttribute,
      AttributeDescriptor<?> target,
      ProxyTransform transform,
      boolean isRead,
      boolean isWrite) {

    if (transform.isContextual()) {
      DataOperator operator = getDataOperatorForDelegate(transform);
      transform.asContextual().setup(entity, operator);
    } else {
      transform.asElementWise().setup(asProxySetupContext(proxyAttribute, target, isRead, isWrite));
    }
  }

  @SuppressWarnings("unchecked")
  private void addProxyAttributeAsymmetric(
      String attrName, Map<String, Object> proxyMap, EntityDescriptor.Builder entityBuilder) {

    final AttributeDescriptor<Object> readTarget;
    final AttributeDescriptor<Object> writeTarget;
    final ProxyTransform readTransform;
    final ProxyTransform writeTransform;
    Map<String, Object> write = toMapOrNull(proxyMap.get(WRITE));
    Map<String, Object> read = toMapOrNull(proxyMap.get(READ));
    AttributeDescriptor<Object> original = null;
    if (write == null || read == null) {
      // we need to load the original attribute, which must have been
      // loaded (must contain `scheme`)
      original = (AttributeDescriptor<Object>) entityBuilder.getAttribute(attrName);
    }
    if (read != null) {
      readTarget =
          Optional.ofNullable(read.get("from"))
              .map(Object::toString)
              .map(entityBuilder::getAttribute)
              .map(a -> (AttributeDescriptor<Object>) a)
              .orElseThrow(
                  () -> new IllegalStateException("Invalid state: `read.from` must not be null"));
    } else {
      readTarget = original;
    }
    if (write != null) {
      writeTarget =
          Optional.ofNullable(write.get("into"))
              .map(Object::toString)
              .map(entityBuilder::getAttribute)
              .map(a -> (AttributeDescriptor<Object>) a)
              .orElseThrow(
                  () -> new IllegalStateException("Invalid state: `write.into` must not be null"));
    } else {
      writeTarget = original;
    }

    readTransform =
        readTarget == original ? ElementWiseProxyTransform.identity() : getProxyTransform(read);
    writeTransform =
        writeTarget == original ? ElementWiseProxyTransform.identity() : getProxyTransform(write);
    URI schemeURI = readProxySchemeOptional(attrName, proxyMap, writeTarget, readTarget);
    entityBuilder.addAttribute(
        AttributeDescriptor.newProxy(
            attrName,
            readTarget,
            readTransform,
            writeTarget,
            writeTransform,
            schemeURI,
            requireValueSerializerFactory(schemeURI).getValueSerializer(schemeURI)));
  }

  private URI readProxySchemeOptional(
      String proxyName,
      Map<String, Object> settings,
      AttributeDescriptor<?> write,
      AttributeDescriptor<?> read) {

    return Optional.ofNullable(settings.get(SCHEME))
        .map(Object::toString)
        .map(this::asSchemeUri)
        .orElseGet(
            () -> {
              Preconditions.checkArgument(
                  write.getSchemeUri().equals(read.getSchemeUri()),
                  "Proxy %s does not specify explicit scheme, and read scheme URI %s does not match write scheme URI %s",
                  proxyName,
                  read.getSchemeUri(),
                  write.getSchemeUri());
              return write.getSchemeUri();
            });
  }

  private ProxyTransform getProxyTransform(Map<String, Object> map) {
    return Optional.ofNullable(map.get("apply"))
        .map(Object::toString)
        .map(s -> Classpath.newInstance(s, ProxyTransform.class))
        .orElseThrow(() -> new IllegalArgumentException("Missing required field `apply'"));
  }

  private void loadRegular(
      String entityName,
      String attrName,
      Map<String, Object> settings,
      EntityDescriptor.Builder entityBuilder) {

    final Object scheme =
        Objects.requireNonNull(
            settings.get(SCHEME),
            "Missing key entities." + entityName + ".attributes." + attrName + ".scheme");

    URI schemeUri = asSchemeUri(scheme.toString());
    validateSerializerFactory(schemeUri);
    entityBuilder.addAttribute(
        AttributeDescriptor.newBuilder(this)
            .setEntity(entityName)
            .setName(attrName)
            .setSchemeUri(schemeUri)
            .build());
  }

  private URI asSchemeUri(String scheme) {
    String schemeStr = scheme;
    if (schemeStr.indexOf(':') == -1) {
      schemeStr += ":///";
    }
    return URI.create(schemeStr);
  }

  private void validateSerializerFactory(URI schemeUri) {
    // validate that the scheme serializer doesn't throw exceptions
    // ignore the return value
    try {
      if (isShouldValidate(Validate.SERIALIZERS)) {
        requireValueSerializerFactory(schemeUri).getValueSerializer(schemeUri).isUsable();
      }
    } catch (Exception ex) {
      throw new IllegalStateException("Cannot use serializer for URI " + schemeUri, ex);
    }
  }

  private ValueSerializerFactory requireValueSerializerFactory(URI schemeUri) {
    return getValueSerializerFactory(schemeUri.getScheme())
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Unable to get ValueSerializerFactory for scheme "
                        + schemeUri.getScheme()
                        + "."));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> toMap(String key, Object value) {
    return toMap(key, value, true);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> toMap(String key, Object value, boolean required) {
    if (!(value instanceof Map)) {
      if (required) {
        throw new IllegalArgumentException(
            "Key "
                + key
                + " must be object got "
                + (value != null ? value.getClass().getName() : "(null)"));
      }
      return null;
    }
    return (Map<String, Object>) value;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> toMapOrNull(Object value) {
    if (!(value instanceof Map)) {
      return null;
    }
    return (Map<String, Object>) value;
  }

  @Override
  public Optional<EntityDescriptor> findEntity(String name) {
    EntityDescriptor byName = entitiesByName.get(name);
    if (byName != null) {
      return Optional.of(byName);
    }
    return Optional.empty();
  }

  @Override
  public Optional<ValueSerializerFactory> getValueSerializerFactory(String scheme) {

    ValueSerializerFactory serializer = serializersMap.get(scheme);
    if (serializer == null) {
      if (isShouldValidate(Validate.SERIALIZERS)) {
        throw new IllegalArgumentException("Missing serializer for scheme " + scheme);
      } else {
        return Optional.empty();
      }
    }
    return Optional.of(serializer);
  }

  @Override
  public boolean isShouldValidate(Validate what) {
    return what.getFlag() == 0 || (this.validateFlags & what.getFlag()) != 0;
  }

  @Override
  public void drop() {
    LocalInstance.drop();
    VersionedCaching.drop();
    ConfigRepository.dropCached();
  }

  private void readAttributeFamilies(Config cfg) {

    if (entitiesByName.isEmpty()) {
      // no loaded entities, no more stuff to read
      return;
    }
    Map<String, Object> attributeFamilyMap =
        toMap(
            ATTRIBUTE_FAMILIES,
            Objects.requireNonNull(
                    cfg.root().get(ATTRIBUTE_FAMILIES),
                    String.format("Missing required [%s] setting", ATTRIBUTE_FAMILIES))
                .unwrapped());

    for (Map.Entry<String, Object> e : attributeFamilyMap.entrySet()) {
      String name = e.getKey();
      Map<String, Object> storage = toMap(ATTRIBUTE_FAMILIES + "." + name, e.getValue());

      try {
        loadSingleFamily(name, false, storage);
      } catch (Exception ex) {
        throw new IllegalArgumentException(
            "Failed to read settings of attribute family " + name, ex);
      }
    }
  }

  private void loadSingleFamily(String name, boolean overwrite, Map<String, Object> cfg)
      throws URISyntaxException {

    cfg = flatten(cfg);
    boolean isDisabled =
        Optional.ofNullable(cfg.get(DISABLED))
            .map(Object::toString)
            .map(Boolean::valueOf)
            .orElse(false);
    if (isDisabled) {
      log.info("Skipping load of disabled family {}", name);
      return;
    }
    final String entity = Objects.requireNonNull(cfg.get(ENTITY)).toString();
    final String filter = toString(cfg.get(FILTER));
    // type is one of the following:
    // * commit (stream commit log)
    // * append (append-only stream or batch)
    // * random-access (random-access read-write)
    final StorageType type = StorageType.of((String) cfg.get(TYPE));
    final AccessType access =
        AccessType.from(Optional.ofNullable(cfg.get(ACCESS)).orElse(READ_ONLY));
    final List<String> attributes =
        toList(
            Objects.requireNonNull(
                cfg.get(ATTRIBUTES),
                () ->
                    String.format(
                        "Missing required field `%s' in attributeFamily %s", ATTRIBUTES, name)));
    final URI storageUri =
        new URI(
            Objects.requireNonNull(
                    cfg.get(STORAGE),
                    () ->
                        String.format(
                            "Missing required field `%s' in attribute family %s", STORAGE, name))
                .toString());
    final EntityDescriptor entDesc = findEntityRequired(entity);
    AttributeFamilyDescriptor.Builder family =
        AttributeFamilyDescriptor.newBuilder()
            .setEntity(entDesc)
            .setName(name)
            .setType(type)
            .setAccess(access)
            .setStorageUri(storageUri)
            .setCfg(cfg)
            .setSource((String) cfg.get(FROM));
    Collection<AttributeDescriptor<?>> allAttributes = new HashSet<>();
    for (String attr : attributes) {
      // attribute descriptors affected by this settings
      final List<AttributeDescriptor<?>> attrDescs;
      attrDescs = searchAttributesMatching(attr, entDesc, false, false);
      allAttributes.addAll(attrDescs);
    }
    if (!filter.isEmpty() && !readonly) {
      insertFilterIfPossible(allAttributes, type, filter, name, family);
    }
    allAttributes.forEach(family::addAttribute);
    insertFamily(family.build(), overwrite);
  }

  private void insertFilterIfPossible(
      Collection<AttributeDescriptor<?>> allAttributes,
      StorageType type,
      String filter,
      String familyName,
      AttributeFamilyDescriptor.Builder family) {

    boolean allProtected =
        allAttributes.stream().map(a -> !a.isPublic()).reduce(true, (a, b) -> a && b);

    if (type == StorageType.PRIMARY) {
      if (!allProtected) {
        throw new IllegalArgumentException(
            "Primary storage for non-protected attributes "
                + allAttributes
                + " cannot have filters");
      } else {
        log.info(
            "Allowing filter {} in PRIMARY family {} for protected targets {}",
            filter,
            familyName,
            allAttributes);
      }
    }

    family.setFilter(Classpath.newInstance(filter, StorageFilter.class));
  }

  private void insertFamily(AttributeFamilyDescriptor family, boolean overwrite) {
    family
        .getAttributes()
        .forEach(
            a -> {
              Set<AttributeFamilyDescriptor> families =
                  attributeToFamily.computeIfAbsent(a, k -> new HashSet<>());
              if (family.getType() == StorageType.PRIMARY) {
                Optional<AttributeFamilyDescriptor> otherPrimary =
                    families.stream().filter(af -> af.getType() == StorageType.PRIMARY).findAny();
                if (otherPrimary.isPresent()) {
                  if (!overwrite) {
                    RuntimeException ex =
                        new IllegalStateException(
                            "Attribute "
                                + a
                                + " already has primary family "
                                + otherPrimary.get()
                                + " while adding "
                                + family);
                    log.error("Failed to insert family {}", family, ex);
                    throw ex;
                  } else {
                    removeFamily(otherPrimary.get());
                  }
                }
              }
              if (!families.add(family) && !overwrite) {
                throw new IllegalArgumentException(
                    "Attribute family named " + family.getName() + " already exists");
              }
            });
    allCreatedFamilies.putIfAbsent(family.getName(), family);
    log.debug(
        "Added family {} of type {} and access {}", family, family.getType(), family.getAccess());
  }

  void removeFamily(AttributeFamilyDescriptor family) {
    family.getAttributes().forEach(attr -> getFamiliesForAttribute(attr).remove(family));
  }

  private void createReplicationFamilies(Map<String, Replication> replications) {
    for (Map.Entry<String, Replication> e : replications.entrySet()) {
      String replicationName = e.getKey();
      Replication repl = e.getValue();
      boolean readOnly = repl.isReadOnly();
      Map<String, Object> targets = repl.getTargets();
      Map<String, Object> source = repl.getSource();
      Map<String, Object> via = repl.getVia();
      EntityDescriptorImpl entity = repl.getEntity();
      Collection<String> attrNames = repl.getAttrs();
      List<AttributeDescriptorBase<?>> attrs =
          attrNames
              .stream()
              .map(a -> findAttributeRequired(entity, a))
              .collect(Collectors.toList());
      AttributeFamilyDescriptor sourceFamily = repl.getFamily();
      boolean readNonReplicated = repl.isReadNonReplicated();

      try {
        // OK, we have single family for this replication
        // we will create the following primary commit-logs:
        // 1) read-only `replication_<name>_source` for source data
        // 2) write-only `replication_target_<name>_<target>` for targets
        // 3) `replication_<name>_write` for writes to original attributes
        // 4) `replication_<name>_replicated` for replicated data
        createReplicationCommitLog(
            replicationName,
            entity,
            attrs,
            source,
            via,
            targets,
            sourceFamily,
            readNonReplicated,
            readOnly);

        // remove the original attribute and replace it with proxy
        // 1) on write: write to _<replication_name>_write$attr
        //              (or original attribute if readOnly)
        // 2) on read: read from _<replication_name>_replicated$attr,
        //             or _<replication_name>_write$attr
        //             if configured to read non-replicated data only
        bindReplicationProxies(replicationName, entity, attrNames, readNonReplicated, readOnly);

        if (!readOnly && !repl.isReadNonReplicated()) {
          // add the following renaming transformations
          // 1) _<replication_name>_read$attr -> _<replication_name>_replicated$attr
          // 2) _<replication_name>_write$attr -> _<replication_name>_replicated$attr
          // 3) _<replication_name>_write$attr -> _<replication_name>_<target>$attr
          createReplicationTransformations(
              replicationName, entity.getName(), attrNames, targets.keySet());
        } else {
          log.info(
              "Skipping creation of transformations for {}: readOnly: "
                  + "{}, readNonReplicated: {}",
              replicationName,
              readOnly,
              repl.isReadNonReplicated());
        }

      } catch (URISyntaxException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private void createReplicationCommitLog(
      String name,
      EntityDescriptor entity,
      Collection<AttributeDescriptorBase<?>> attrs,
      Map<String, Object> source,
      Map<String, Object> via,
      Map<String, Object> targets,
      AttributeFamilyDescriptor sourceFamily,
      boolean readNonReplicated,
      boolean readOnly)
      throws URISyntaxException {

    // create parent settings for families
    List<String> attrList =
        attrs.stream().map(AttributeDescriptor::getName).collect(Collectors.toList());

    // remove primary family of the original attribute, it will be just proxy
    attrs.forEach(
        attr ->
            this.attributeToFamily.compute(
                attr,
                (tmp, current) -> {
                  Set<AttributeFamilyDescriptor> updated =
                      current
                          .stream()
                          .filter(af -> !af.getName().equals(sourceFamily.getName()) || readOnly)
                          .collect(Collectors.toSet());
                  return updated.isEmpty() ? null : updated;
                }));

    Map<String, Object> cfgMapTemplate = new HashMap<>();
    cfgMapTemplate.put(ENTITY, entity.getName());
    cfgMapTemplate.put(ATTRIBUTES, attrList);
    cfgMapTemplate.put(TYPE, "primary");

    if (!source.isEmpty()) {
      // create source
      Map<String, Object> cfg = new HashMap<>(cfgMapTemplate);
      cfg.put(ACCESS, "commit-log, read-only");
      cfg.putAll(source);
      cfg.put(
          ATTRIBUTES,
          attrList.stream().map(a -> toReplicationReadName(name, a)).collect(Collectors.toList()));
      loadSingleFamily(String.format("replication_%s_source", name), true, cfg);
    }
    if (!readNonReplicated || !source.isEmpty()) {
      final AttributeFamilyDescriptor resolvedRead;
      if (sourceFamily.isProxy()) {
        resolvedRead = ((AttributeFamilyProxyDescriptor) sourceFamily).getTargetFamilyWrite();
      } else {
        resolvedRead = sourceFamily;
      }
      AttributeFamilyDescriptor.Builder builder =
          resolvedRead
              .toBuilder()
              .setName(String.format("replication_%s_replicated", name))
              .clearAttributes();
      attrList
          .stream()
          .map(
              a ->
                  findAttributeRequired(
                      entity,
                      toReplicationProxyName(
                          name,
                          strippingReplPrefix(resolveProxyTarget(entity, a, true).getName()))))
          .forEach(builder::addAttribute);
      insertFamily(builder.build(), true);
    }
    if (!readOnly) {
      createLocalWriteCommitLog(entity, name, cfgMapTemplate, via, attrList);
      for (Map.Entry<String, Object> tgt : targets.entrySet()) {
        Map<String, Object> cfg = new HashMap<>(cfgMapTemplate);
        cfg.putAll(toMap(tgt.getKey(), tgt.getValue()));
        cfg.put(ACCESS, "write-only");
        cfg.put(
            ATTRIBUTES,
            attrList
                .stream()
                .map(a -> toReplicationTargetName(name, tgt.getKey(), a))
                .collect(Collectors.toList()));
        loadSingleFamily(String.format("replication_target_%s_%s", name, tgt.getKey()), true, cfg);
      }
    }
  }

  private void createLocalWriteCommitLog(
      EntityDescriptor entity,
      String replicationName,
      Map<String, Object> cfgMapTemplate,
      Map<String, Object> via,
      List<String> attrList)
      throws URISyntaxException {

    Preconditions.checkArgument(
        !via.isEmpty(), "Missing required settings for replication `via` settings");
    // create family for writes and replication
    Map<String, Object> cfg = new HashMap<>(cfgMapTemplate);
    // this can be overridden
    cfg.put(ACCESS, "commit-log");
    cfg.putAll(via);
    cfg.put(
        ATTRIBUTES,
        attrList
            .stream()
            .map(
                a ->
                    toReplicationWriteName(
                        replicationName, resolveProxyTarget(entity, a, false).getName()))
            .collect(Collectors.toList()));
    loadSingleFamily(String.format("replication_%s_write", replicationName), true, cfg);
  }

  @SuppressWarnings("unchecked")
  private static AttributeDescriptor<?> resolveProxyTarget(
      EntityDescriptor entity, String attr, boolean read) {

    AttributeDescriptor<?> result = findAttributeRequired(entity, attr);
    while (result.isProxy()) {
      result =
          read
              ? ((AttributeDescriptorBase<?>) result).toProxy().getReadTarget()
              : ((AttributeDescriptorBase<?>) result).toProxy().getWriteTarget();
    }
    return result;
  }

  private void createReplicationTransformations(
      String name, String entityName, Collection<String> attrNames, Set<String> targets) {

    EntityDescriptor entity =
        findEntity(entityName)
            .orElseThrow(() -> new IllegalStateException("Missing entity " + entityName));

    List<AttributeDescriptor<?>> attrs =
        attrNames.stream().map(a -> findAttributeRequired(entity, a)).collect(Collectors.toList());

    AttributeFamilyDescriptor write =
        findFamilyRequired(String.format("replication_%s_write", name));

    AttributeFamilyDescriptor replicated =
        findFamilyRequired(String.format("replication_%s_replicated", name));

    findFamily(String.format("replication_%s_source", name))
        .ifPresent(s -> createRemoteReadTransform(name, attrs, entity, s, replicated));

    createLocalWriteTransform(name, entity, write, replicated);

    for (String tgt : targets) {
      createTargetTransform(name, tgt, attrs, entity, write);
    }
  }

  private void createRemoteReadTransform(
      String replicationName,
      List<AttributeDescriptor<?>> attrs,
      EntityDescriptor entity,
      AttributeFamilyDescriptor source,
      AttributeFamilyDescriptor replicated) {

    // incoming data
    String transform = CamelCase.apply(String.format("_%s_read", replicationName), false);
    String replPrefix = CamelCase.apply(String.format("_%s_replicated$", replicationName), false);

    Map<AttributeDescriptor<?>, AttributeDescriptor<?>> sourceMapping;
    sourceMapping = getReplMapping(entity, attrs, replPrefix, false);

    this.transformations.put(
        transform,
        TransformationDescriptor.newBuilder()
            .setName(transform)
            .addAttributes(source.getAttributes())
            .setEntity(entity)
            .setFilter(replicated.getFilter())
            .setTransformation(
                renameTransform(
                    sourceMapping::get,
                    (input, desc) -> {
                      String raw = strippingReplPrefix(input);
                      // each incoming attribute is proxy
                      AttributeProxyDescriptor<?> proxyDesc;
                      proxyDesc = ((AttributeDescriptorBase<?>) desc).toProxy();
                      return strippingReplPrefix(
                          proxyDesc.getWriteTransform().asElementWise().fromProxy(raw));
                    }))
            .build());
  }

  private void createTargetTransform(
      String replicationName,
      String target,
      List<AttributeDescriptor<?>> attrs,
      EntityDescriptor entity,
      AttributeFamilyDescriptor write) {

    AttributeFamilyDescriptor targetFamily =
        findFamilyRequired(String.format("replication_target_%s_%s", replicationName, target));

    String transform = CamelCase.apply(String.format("_%s_%s", replicationName, target));
    String replPrefix = transform + "$";
    Map<AttributeDescriptor<?>, AttributeDescriptor<?>> sourceMapping = new HashMap<>();
    Map<AttributeDescriptor<?>, AttributeDescriptor<?>> sourceToOrig = new HashMap<>();

    // store targets using original non-proxied name
    attrs
        .stream()
        .map(a -> findAttributeRequired(entity, a.getName()).toProxy())
        .forEach(
            a -> {
              String source = strippingReplPrefix(a.getReadTarget().getName());
              AttributeDescriptorBase<?> sourceAttr;
              sourceAttr = findAttributeRequired(entity, source);
              String renamed = replPrefix + a.getName();
              sourceMapping.put(sourceAttr, findAttributeRequired(entity, renamed));
              sourceToOrig.put(sourceAttr, a);
            });

    this.transformations.put(
        transform,
        TransformationDescriptor.newBuilder()
            .setName(transform)
            .addAttributes(write.getAttributes())
            .setEntity(entity)
            .setFilter(targetFamily.getFilter())
            .setTransformation(
                renameTransform(
                    sourceMapping::get,
                    (input, desc) -> {
                      String raw = strippingReplPrefix(input);
                      AttributeProxyDescriptor<?> proxyDesc;
                      proxyDesc = ((AttributeDescriptorBase<?>) sourceToOrig.get(desc)).toProxy();
                      return strippingReplPrefix(
                          proxyDesc.getReadTransform().asElementWise().toProxy(raw));
                    }))
            .build());
  }

  private void createLocalWriteTransform(
      String replicationName,
      EntityDescriptor entity,
      AttributeFamilyDescriptor write,
      AttributeFamilyDescriptor replicated) {

    // local writes
    String transform = CamelCase.apply(String.format("_%s_replicated", replicationName), false);
    String replPrefix = transform + "$";
    Map<AttributeDescriptor<?>, AttributeDescriptor<?>> sourceMapping;
    List<AttributeDescriptor<?>> readAttrs = strippingReplPrefix(entity, write.getAttributes());
    readAttrs.addAll(write.getAttributes());
    sourceMapping = getReplMapping(entity, readAttrs, replPrefix, false);

    this.transformations.put(
        transform,
        TransformationDescriptor.newBuilder()
            .setName(transform)
            .addAttributes(write.getAttributes())
            .setEntity(entity)
            .setFilter(replicated.getFilter())
            .setTransformation(
                renameTransform(
                    src ->
                        Objects.requireNonNull(
                            sourceMapping.get(src),
                            () -> "Missing " + src + " in " + sourceMapping),
                    // store under original name
                    (a, desc) -> a))
            .build());
  }

  private EntityDescriptorImpl findEntityRequired(String entity) {
    return (EntityDescriptorImpl)
        findEntity(entity)
            .orElseThrow(() -> new IllegalStateException("Missing required entity " + entity));
  }

  @SuppressWarnings("unchecked")
  private static <T> AttributeDescriptorBase<T> findAttributeRequired(
      EntityDescriptor entity, String attribute) {

    return (AttributeDescriptorBase<T>)
        entity
            .findAttribute(attribute, true)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Entity " + entity + " is missing attribute " + attribute));
  }

  private static Map<AttributeDescriptor<?>, AttributeDescriptor<?>> getReplMapping(
      EntityDescriptor entity,
      Collection<AttributeDescriptor<?>> attrs,
      String mappedPrefix,
      boolean read) {

    return attrs
        .stream()
        .map(
            a -> {
              AttributeDescriptorBase<?> base = (AttributeDescriptorBase<?>) a;
              final String attrName;
              if (base.isProxy()) {
                attrName =
                    read
                        ? base.toProxy().getReadTarget().getName()
                        : base.toProxy().getWriteTarget().getName();
              } else {
                attrName = a.getName();
              }
              String renamed = renameReplicated(mappedPrefix, attrName);
              return Pair.of(
                  a,
                  entity
                      .findAttribute(renamed, true)
                      .orElseThrow(
                          () -> new IllegalStateException("Missing attribute " + renamed)));
            })
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  private static String renameReplicated(String prefix, String input) {
    int dollar = input.indexOf('$');
    if (dollar < 0) {
      return prefix + input;
    }
    return prefix + input.substring(dollar + 1);
  }

  private static String strippingReplPrefix(String input) {
    int dollar = input.indexOf('$');
    if (dollar < 0) {
      log.debug("Input name {} has no dollar '$' character. Ignoring.", input);
      return input;
    }
    return input.substring(dollar + 1);
  }

  private static List<AttributeDescriptor<?>> strippingReplPrefix(
      EntityDescriptor entity, List<AttributeDescriptor<?>> what) {

    return what.stream()
        .map(AttributeDescriptor::getName)
        .map(ConfigRepository::strippingReplPrefix)
        .map(a -> findAttributeRequired(entity, a))
        .collect(Collectors.toList());
  }

  private static ProxyTransform strippingReplPrefixTransform() {
    return ElementWiseProxyTransform.droppingUntilCharacter('$', "");
  }

  private static Transformation renameTransform(
      UnaryFunction<AttributeDescriptor<?>, AttributeDescriptor<?>> descTransform,
      BiFunction<String, AttributeDescriptor<?>, String> nameTransform) {

    return new RenameTransformation(descTransform, nameTransform);
  }

  private Optional<AttributeFamilyDescriptor> findFamily(String name) {
    return getAllFamilies().filter(af -> af.getName().equals(name)).findFirst();
  }

  private AttributeFamilyDescriptor findFamilyRequired(String name) {
    return findFamily(name)
        .orElseThrow(() -> new IllegalStateException("Missing required family '" + name + "'"));
  }

  private void bindReplicationProxies(
      String name,
      EntityDescriptorImpl entity,
      Collection<String> attrNames,
      boolean readNonReplicatedOnly,
      boolean readOnly) {

    List<AttributeDescriptor<?>> attrs =
        attrNames.stream().map(a -> findAttributeRequired(entity, a)).collect(Collectors.toList());

    attrs
        .stream()
        .filter(a -> !((AttributeDescriptorBase<?>) a).isProxy())
        .forEach(a -> bindSingleReplicationProxy(name, entity, a, readNonReplicatedOnly, readOnly));

    buildProxyOrdering(
            attrs
                .stream()
                .flatMap(
                    a ->
                        entity
                            .findAttribute(a.getName(), true)
                            .map(Stream::of)
                            .orElse(Stream.empty()))
                .filter(a -> ((AttributeDescriptorBase<?>) a).isProxy())
                .map(a -> ((AttributeDescriptorBase<?>) a).toProxy())
                .collect(Collectors.toList()))
        .forEach(a -> bindSingleReplicationProxy(name, entity, a, readNonReplicatedOnly, readOnly));
  }

  @SuppressWarnings("unchecked")
  private void bindSingleReplicationProxy(
      String replicationName,
      EntityDescriptorImpl entity,
      AttributeDescriptor<?> proxy,
      boolean readNonReplicatedOnly,
      boolean readOnly) {

    if (proxy.isProxy()) {
      // recursively bind targets
      @SuppressWarnings("unchecked")
      AttributeProxyDescriptor<Object> targetProxy =
          ((AttributeDescriptorBase<Object>) proxy).toProxy();
      Set<String> toRebind =
          Stream.of(targetProxy.getReadTarget(), targetProxy.getWriteTarget())
              .filter(a -> !((AttributeDescriptorBase<?>) a).isReplica())
              .filter(a -> !a.equals(proxy))
              .map(AttributeDescriptor::getName)
              .collect(Collectors.toSet());
      bindReplicationProxies(replicationName, entity, toRebind, readNonReplicatedOnly, readOnly);
      replaceAttribute(
          entity,
          AttributeDescriptor.newProxy(
              proxy.getName(),
              targetProxy.getReadTarget(),
              targetProxy.getReadTransform(),
              targetProxy.getWriteTarget(),
              targetProxy.getWriteTransform(),
              targetProxy.getReadTarget().getSchemeUri(),
              targetProxy.getReadTarget().getValueSerializer()));
    } else {
      final AttributeDescriptor<Object> source;
      final AttributeDescriptor<Object> target;
      source =
          findAttributeRequired(
              entity,
              readNonReplicatedOnly && !readOnly
                  ? toReplicationWriteName(replicationName, proxy.getName())
                  : toReplicationProxyName(replicationName, proxy.getName()));
      target =
          readOnly
              ? findAttributeRequired(entity, proxy.getName())
              : findAttributeRequired(
                  entity, toReplicationWriteName(replicationName, proxy.getName()));
      replaceAttribute(
          entity,
          AttributeDescriptor.newProxy(
              proxy.getName(),
              source,
              strippingReplPrefixTransform(),
              target,
              // store under original name
              strippingReplPrefixTransform(),
              target.getSchemeUri(),
              target.getValueSerializer()));
    }
  }

  private List<AttributeDescriptor<?>> searchAttributesMatching(
      String attr, EntityDescriptor entDesc, boolean allowReplicated, boolean allowProxies) {

    return searchAttributesMatching(attr, entDesc, allowReplicated, true, allowProxies);
  }

  private List<AttributeDescriptor<?>> searchAttributesMatching(
      String attr,
      EntityDescriptor entDesc,
      boolean allowReplicated,
      boolean includeProtected,
      boolean allowProxies) {

    List<AttributeDescriptor<?>> attrDescs;
    if (attr.equals("*")) {
      // this means all attributes of entity
      attrDescs =
          entDesc
              .getAllAttributes(includeProtected)
              .stream()
              .filter(a -> !((AttributeDescriptorBase<?>) a).isReplica() || allowReplicated)
              .filter(a -> !((AttributeDescriptorBase<?>) a).isProxy() || allowProxies)
              .collect(Collectors.toList());
    } else {
      attrDescs =
          Collections.singletonList(
              entDesc
                  .findAttribute(attr, includeProtected)
                  .orElseThrow(
                      () -> new IllegalArgumentException("Cannot find attribute " + attr)));
    }
    return attrDescs;
  }

  private void loadProxiedFamilies() {
    loadProxiedFamilies(false);
  }

  @SuppressWarnings("unchecked")
  private void loadProxiedFamilies(boolean all) {
    Map<
            Pair<AttributeFamilyDescriptor, AttributeFamilyDescriptor>,
            List<AttributeProxyDescriptor<?>>>
        readWriteToAttr;

    List<AttributeProxyDescriptor<?>> attributes =
        getAllEntities()
            .flatMap(e -> e.getAllAttributes(true).stream())
            .filter(AttributeDescriptor::isProxy)
            .map(AttributeDescriptor::asProxy)
            .filter(a -> all || !a.isReplica())
            .collect(Collectors.toList());

    // build dependency ordering, because we might have dependency
    // chain in the families, and we need to rebind them
    // in the dependency order (bottom-up)
    Collection<AttributeProxyDescriptor<?>> dependencyOrdered =
        all ? buildProxyOrdering(attributes) : attributes;

    // seek for attributes with the same read-write families
    // and create new family for each such pair containing
    // the attributes involved, thus reducing the number of created
    // families (attributes stored in the same families will be
    // proxied with single family, which will enable efficient
    // reads of such attributes)
    readWriteToAttr =
        (Map)
            dependencyOrdered
                .stream()
                .flatMap(
                    p -> {
                      AttributeDescriptor<?> writeTarget = p.getWriteTarget();
                      AttributeDescriptor<?> readTarget = p.getReadTarget();
                      // find write family
                      AttributeFamilyDescriptor writeFamily =
                          getFamiliesForAttribute(writeTarget)
                              .stream()
                              .filter(af -> af.getType() == StorageType.PRIMARY)
                              .findFirst()
                              .orElseThrow(
                                  () ->
                                      new IllegalStateException(
                                          String.format(
                                              "Missing primary storage for %s. Found families %s",
                                              writeTarget, getFamiliesForAttribute(writeTarget))));
                      return getFamiliesForAttribute(readTarget)
                          .stream()
                          .map(af -> Pair.of(p, Pair.of(af, writeFamily)));
                    })
                .collect(
                    Collectors.groupingBy(
                        Pair::getSecond, Collectors.mapping(Pair::getFirst, Collectors.toList())));

    for (AttributeProxyDescriptor<?> attr : dependencyOrdered) {
      // prevent ConcurrentModificationException
      List<AttributeFamilyDescriptor> createdFamilies = new ArrayList<>();

      readWriteToAttr
          .entrySet()
          .stream()
          .filter(e -> e.getValue().contains(attr))
          .forEach(
              e -> {
                List<AttributeProxyDescriptor<?>> proxyList = e.getValue();
                // cartesian product of read x write families
                for (AttributeFamilyDescriptor read :
                    getFamiliesForAttribute(attr.getReadTarget())) {
                  for (AttributeFamilyDescriptor write :
                      getFamiliesForAttribute(attr.getWriteTarget())) {

                    for (AttributeProxyDescriptor<?> a : proxyList) {
                      createdFamilies.add(
                          AttributeFamilyProxyDescriptor.of(proxyList, read, write));
                    }
                  }
                }
              });
      createdFamilies.forEach(family -> insertFamily(family, true));
    }
  }

  private LinkedHashSet<AttributeProxyDescriptor<?>> buildProxyOrdering(
      Collection<? extends AttributeDescriptor<?>> attributes) {

    LinkedHashSet<AttributeProxyDescriptor<?>> dependencyOrdered;
    dependencyOrdered = new LinkedHashSet<>();
    List<AttributeProxyDescriptor<?>> proxies =
        attributes
            .stream()
            .map(a -> ((AttributeDescriptorBase<?>) a).toProxy())
            .collect(Collectors.toList());

    while (dependencyOrdered.size() != proxies.size()) {
      boolean modified =
          proxies
              .stream()
              .filter(
                  p ->
                      (!((AttributeDescriptorBase<?>) p.getReadTarget()).isProxy()
                              || dependencyOrdered.contains(p.getReadTarget())
                              // the dependency can be self-resolved in case of
                              // read-only replications
                              || p.getReadTarget().equals(p))
                          && (!((AttributeDescriptorBase<?>) p.getWriteTarget()).isProxy()
                              || dependencyOrdered.contains(p.getWriteTarget())
                              || p.getWriteTarget().equals(p)))
              .map(dependencyOrdered::add)
              .anyMatch(a -> a);
      if (!modified) {
        throw new IllegalStateException("Cannot determine the proxy ordering. Fix code!");
      }
    }
    return dependencyOrdered;
  }

  private void readTransformations(Config cfg) {

    if (entitiesByName.isEmpty()) {
      // no loaded entities, no more stuff to read
      return;
    }
    Map<String, Object> cfgTransforms =
        Optional.ofNullable(cfg.root().get("transformations"))
            .map(v -> toMap("transformations", v.unwrapped()))
            .orElse(null);

    if (cfgTransforms == null) {
      log.info("Skipping empty transformations configuration.");
      return;
    }

    cfgTransforms.forEach(
        (name, v) -> {
          Map<String, Object> transformation = toMap(name, v);

          boolean disabled =
              Optional.ofNullable(transformation.get(DISABLED))
                  .map(d -> Boolean.valueOf(d.toString()))
                  .orElse(false);

          if (disabled) {
            log.info("Skipping load of disabled transformation {}", name);
            return;
          }

          EntityDescriptor entity =
              findEntity(readStr(ENTITY, transformation, name))
                  .orElseThrow(
                      () ->
                          new IllegalArgumentException(
                              String.format(
                                  "Entity `%s` doesn't exist", transformation.get(ENTITY))));

          Transformation t =
              Classpath.newInstance(readStr("using", transformation, name), Transformation.class);

          List<AttributeDescriptor<?>> attrs =
              readList(ATTRIBUTES, transformation, name)
                  .stream()
                  .flatMap(a -> searchAttributesMatching(a, entity, true, true).stream())
                  .collect(Collectors.toList());

          TransformationDescriptor.Builder desc =
              TransformationDescriptor.newBuilder()
                  .setName(name)
                  .addAttributes(attrs)
                  .setEntity(entity)
                  .setTransformation(t);

          Optional.ofNullable(transformation.get(FILTER))
              .map(Object::toString)
              .map(s -> Classpath.newInstance(s, StorageFilter.class))
              .ifPresent(desc::setFilter);

          TransformationDescriptor transformationDescriptor = desc.build();
          setupTransform(transformationDescriptor.getTransformation(), transformation);
          this.transformations.put(name, transformationDescriptor);
        });
  }

  private void setupTransform(Transformation transformation, Map<String, Object> cfg) {
    if (transformation.isContextual()) {
      final DataOperator op = getDataOperatorForDelegate(transformation);
      transformation.asContextualTransform().setup(this, op, cfg);
    } else {
      transformation.asElementWiseTransform().setup(this, cfg);
    }
  }

  private DataOperator getDataOperatorForDelegate(DataOperatorAware delegate) {
    Optional<DataOperatorFactory<?>> factory =
        Streams.stream(getDataOperatorFactories()).filter(delegate::isDelegateOf).findFirst();
    Preconditions.checkState(
        factory.isPresent(),
        "Transformation %s does not have delegating DataOperator available",
        delegate);
    Optional<DataOperator> current =
        operators.stream().filter(op -> factory.get().isOfType(op.getClass())).findAny();
    return current.orElseGet(() -> cacheDataOperator(factory.get().create(this)));
  }

  private static String readStr(String key, Map<String, Object> map, String name) {
    return Optional.ofNullable(map.get(key))
        .map(Object::toString)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Missing required field `%s` in `%s`", key, name)));
  }

  @SuppressWarnings("unchecked")
  private static List<String> readList(String key, Map<String, Object> map, String name) {

    return Optional.ofNullable(map.get(key))
        .map(
            v -> {
              if (v instanceof List) {
                return (List<Object>) v;
              }
              throw new IllegalArgumentException(
                  String.format("Key `%s` in `%s` must be list", key, name));
            })
        .map(l -> l.stream().map(Object::toString).collect(Collectors.toList()))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format("Missing required field `%s` in `%s", key, name)));
  }

  @SuppressWarnings("unchecked")
  private List<String> toList(Object in) {
    if (in instanceof List) {
      return ((List<Object>) in).stream().map(Object::toString).collect(Collectors.toList());
    }
    return Collections.singletonList(in.toString());
  }

  private static String toString(Object what) {
    return what == null ? "" : what.toString();
  }

  /** check validity of the settings */
  private void validate() {
    // validate that each attribute belongs to at least one attribute family
    entitiesByName
        .values()
        .stream()
        .flatMap(d -> d.getAllAttributes(true).stream())
        .filter(a -> !((AttributeDescriptorBase<?>) a).isProxy())
        .filter(
            a -> {
              Set<AttributeFamilyDescriptor> families = attributeToFamily.get(a);
              return families == null || families.isEmpty();
            })
        .findAny()
        .ifPresent(
            a -> {
              throw new IllegalArgumentException(
                  "Attribute " + a.getName() + " of entity " + a.getEntity() + " has no storage");
            });

    // check that no attribute has two primary families
    Map<AttributeDescriptor<?>, AttributeFamilyDescriptor> map = new HashMap<>();
    getAllFamilies()
        .filter(af -> af.getType() == StorageType.PRIMARY)
        .flatMap(af -> af.getAttributes().stream().map(a -> Pair.of(a, af)))
        .forEach(
            p -> {
              AttributeFamilyDescriptor conflict = map.put(p.getFirst(), p.getSecond());
              Preconditions.checkArgument(
                  conflict == null,
                  "Attribute "
                      + p.getFirst()
                      + " has two primary families: ["
                      + conflict
                      + ", "
                      + p.getSecond());
            });

    // check the size of attributes with primary family is equal to all attribute
    // hence all attributes have exactly one primary family
    getAllEntities()
        .flatMap(e -> e.getAllAttributes(true).stream())
        .forEach(
            attr ->
                Preconditions.checkArgument(
                    map.get(attr) != null, "Attribute " + attr + " has no primary family"));
  }

  @Override
  public Stream<AttributeFamilyDescriptor> getAllFamilies() {
    return attributeToFamily.values().stream().flatMap(Collection::stream).distinct();
  }

  @Override
  public Optional<AttributeFamilyDescriptor> findFamilyByName(String name) {
    return Optional.ofNullable(allCreatedFamilies.get(name));
  }

  private Set<AttributeFamilyDescriptor> getFamiliesForAttribute(
      AttributeDescriptor<?> attr, boolean errorOnEmpty) {

    Set<AttributeFamilyDescriptor> families = attributeToFamily.get(attr);
    if (errorOnEmpty) {
      Preconditions.checkArgument(
          families != null && !families.isEmpty(), "Cannot find any family for attribute " + attr);
      return families;
    }
    return families == null ? Collections.emptySet() : families;
  }

  @Override
  public Set<AttributeFamilyDescriptor> getFamiliesForAttribute(AttributeDescriptor<?> attr) {

    return getFamiliesForAttribute(attr, true);
  }

  public AttributeFamilyDescriptor getPrimaryFamilyFor(AttributeDescriptor<?> attr) {
    return getFamiliesForAttribute(attr)
        .stream()
        .filter(af -> af.getType() == StorageType.PRIMARY)
        .findAny()
        .orElseThrow(
            () -> new IllegalStateException("Attribute " + attr + " has not primary family"));
  }

  @Override
  public Stream<EntityDescriptor> getAllEntities() {
    return entitiesByName.values().stream();
  }

  @Override
  public Map<String, TransformationDescriptor> getTransformations() {
    return Collections.unmodifiableMap(transformations);
  }

  /**
   * Check if this repository is empty.
   *
   * @return {@code true} if this repository is empty
   */
  @Override
  public boolean isEmpty() {
    return entitiesByName.isEmpty();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> flatten(Map<String, Object> map) {
    Map<String, Object> ret = new HashMap<>();
    map.forEach(
        (key, value) -> {
          if (value instanceof Map) {
            final Map<String, Object> flattened = flatten((Map) value);
            flattened.forEach((key1, value1) -> ret.put(key + "." + key1, value1));
          } else {
            ret.put(key, value);
          }
        });
    return ret;
  }

  @Override
  protected void addedDataOperator(DataOperator op) {
    operators.add(op);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConfigRepository that = (ConfigRepository) o;
    return enableCaching == that.enableCaching
        && readonly == that.readonly
        && validateFlags == that.validateFlags
        && loadClasses == that.loadClasses
        && Objects.equals(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(config, enableCaching, readonly, validateFlags, loadClasses);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "@" + System.identityHashCode(this);
  }

  @VisibleForTesting
  static ProxySetupContext asProxySetupContext(
      AttributeDescriptor<?> proxyAttribute,
      AttributeDescriptor<?> targetAttribute,
      boolean isRead,
      boolean isWrite) {
    return new ProxySetupContext() {
      @Override
      public AttributeDescriptor<?> getProxyAttribute() {
        return proxyAttribute;
      }

      @Override
      public AttributeDescriptor<?> getTargetAttribute() {
        return targetAttribute;
      }

      @Override
      public boolean isReadTransform() {
        return isRead;
      }

      @Override
      public boolean isWriteTransform() {
        return isWrite;
      }
    };
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeObject(asFactory());
  }

  private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
    factory = (RepositoryFactory) ois.readObject();
  }

  private Object readResolve() throws ObjectStreamException {
    return Objects.requireNonNull(factory).apply();
  }
}
