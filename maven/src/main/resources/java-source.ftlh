/*
 * The file is generated from ${input_path}.
 * DO NOT EDIT! YOUR CHANGES WILL BE OVERWRITTEN.
 */

package ${java_package};

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.DataOperator;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.repository.RepositoryFactory;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/* operator specific imports */
<#list imports as import>
import ${import};
</#list>

public class ${java_classname} implements Serializable {

  private static final String GENERATED_FROM = ${input_config};

  /**
   * Create instance of {@code ${java_classname}} for production usage.
   **/
  @Deprecated
  public static ${java_classname} of(Config cfg) {
    return new ${java_classname}(ConfigRepository.Builder.of(cfg.withFallback(ConfigFactory.parseString(GENERATED_FROM))));
  }

  /**
   * Create instance of {@code ${java_classname}} for production usage.
   **/
  public static ${java_classname} of(Repository.ConfigFactory factory) {
    return new ${java_classname}(ConfigRepository.Builder.of(
        () -> factory.apply().withFallback(ConfigFactory.parseString(GENERATED_FROM))));
  }

  /**
   * Create instance of {@code ${java_classname}} with specified {@code ConfigRepository.Builder}.
   **/
  public static ${java_classname} of(ConfigRepository.Builder builder) {
    return new ${java_classname}(builder);
  }

  /**
   * Create instance of {@code ${java_classname}} for testing purposes.
   **/
  public static ${java_classname} ofTest(Config cfg) {
    return new ${java_classname}(ConfigRepository.Builder.ofTest(cfg.withFallback(ConfigFactory.parseString(GENERATED_FROM))));
  }

  @FunctionalInterface
  static interface EntityDescriptorProvider {
    EntityDescriptor getDescriptor();
  }

  @FunctionalInterface
  static interface DataOperatorProvider<T extends DataOperator> extends Serializable {

    T getOperator();

  }

<#list operators as operator>
  public class ${operator.classname} implements DataOperatorProvider<${operator.operatorClass}> {

    ${operator.classdef}

    public ${java_classname} parent() {
      return ${java_classname}.this;
    }

  }
  private final ${operator.classname} ${operator.name};
  public static ${operator.classname} ${operator.name}(Config cfg) {
    return of(cfg).${operator.name}();
  }
  public static ${operator.classname} ${operator.name}(ConfigRepository.Builder builder) {
    return of(builder).${operator.name}();
  }
  public ${operator.classname} ${operator.name}() {
    return ${operator.name};
  }
  public ${operator.operatorClass} ${operator.name}Operator() {
    return ${operator.name}.getOperator();
  }
</#list>

<#list entities as entity>

  /**
   * Class wrapping access to attribute data of entity ${entity.name}.
   **/
  public class ${entity.classname}Wrapper implements Serializable {

  <#list entity.attributes as attribute>
    <#if !attribute.wildcard>
    private ${attribute.type} ${attribute.nameCamel} = null;
    @SuppressWarnings("unchecked")
    private final ${attribute.type} ${attribute.nameCamel}Default = (${attribute.type})
      factory.apply().getEntity("${entity.name}").getAttribute("${attribute.name}").getValueSerializer().getDefault();
    </#if>
  </#list> <#-- entity.attributes as attribute -->

    private ${entity.classname}Wrapper() {

    }

    <#list entity.attributes as attribute>
    <#if !attribute.wildcard>
    public ${attribute.type} get${attribute.nameCamel}() {
      return ${attribute.nameCamel} == null ? ${attribute.nameCamel}Default : ${attribute.nameCamel};
    }
    </#if>
    </#list> <#-- entity.attributes as attribute -->

  }

  /**
   * Class wrapping views on entity ${entity.name}
   **/
  public class ${entity.classname} implements Serializable, EntityDescriptorProvider {

    private final EntityDescriptor descriptor = factory.apply().getEntity("${entity.name}");

    @Override
    public EntityDescriptor getDescriptor() {
      return descriptor;
    }

  <#list entity.attributes as attribute>
    <#if attribute.wildcard>
    @SuppressWarnings("unchecked")
    private final AttributeDescriptor<${attribute.type}> ${attribute.name}Descriptor =
      (AttributeDescriptor) descriptor.getAttribute("${attribute.name}.*");
    <#else>
    @SuppressWarnings("unchecked")
    private final AttributeDescriptor<${attribute.type}> ${attribute.name}Descriptor =
      (AttributeDescriptor) descriptor.getAttribute("${attribute.name}");
    </#if> <#-- attribute.wildcard -->

    public AttributeDescriptor<${attribute.type}> get${attribute.nameCamel}Descriptor() {
      return ${attribute.name}Descriptor;
    }
  </#list> <#-- entity.attributes as attribute -->

    private ${entity.classname}() { }

  }


  </#list>

  private final Config cfg;
  private final RepositoryFactory factory;

<#list entities as entity>
  private final ${entity.classname} ${entity.name};
</#list>


  private ${java_classname}(ConfigRepository.Builder builder) {
    this.factory = Objects.requireNonNull(builder).build().asFactory();
    Repository repo = this.factory.apply();
    this.cfg = ((ConfigRepository) repo).getConfig();
<#list operators as operator>
  ${operator.name} = new ${operator.classname}();
</#list>
<#list entities as entity>
    ${entity.name} = new ${entity.classname}();
</#list>
  }

<#list entities as entity>
  public ${entity.classname} get${entity.nameCamel}() {
    return ${entity.name};
  }
</#list>

  public Repository getRepo() {
    return factory.apply();
  }

}

