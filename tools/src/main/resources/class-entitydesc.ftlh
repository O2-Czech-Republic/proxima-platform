
/*
 * AUTO GENERATED! DO NOT EDIT!
 */

<#if groovyPackage != "">
  package ${groovyPackage}
</#if>

import cz.o2.proxima.core.repository.Repository
import cz.o2.proxima.core.repository.AttributeDescriptor
import cz.o2.proxima.core.repository.EntityDescriptor
import cz.o2.proxima.core.storage.StreamElement
import cz.o2.proxima.core.storage.commitlog.Position
import cz.o2.proxima.core.util.Pair
import cz.o2.proxima.direct.core.DirectDataOperator
import cz.o2.proxima.direct.core.randomaccess.KeyValue
import cz.o2.proxima.direct.core.randomaccess.RandomOffset;
import cz.o2.proxima.tools.groovy.AttributeDescriptorProvider
import cz.o2.proxima.tools.groovy.Console
import cz.o2.proxima.tools.groovy.RepositoryProvider
import cz.o2.proxima.tools.groovy.Stream
import cz.o2.proxima.tools.groovy.WindowedStream
import cz.o2.proxima.tools.io.ConsoleRandomReader

@groovy.transform.CompileStatic
class Environment implements RepositoryProvider {

  private final Console console = Console.get()

  <#list entities as entity>

    class ${entity.classname}Descriptor {

      private final Console console = Environment.this.console
      private final cz.o2.proxima.core.repository.EntityDescriptor desc = console.findEntityDescriptor("${entity.name}")
      private final String name = "${entity.name}"
      private final ConsoleRandomReader reader = console.getRandomAccessReader("${entity.name}")

      def cz.o2.proxima.core.repository.EntityDescriptor getDesc() {
        return desc
      }

      def ConsoleRandomReader getReader() {
        return reader
      }

      def String name() {
        return name
      }

      /* All attributes */
      <#list entity.attributes as attribute>
        class ${attribute.classname}Descriptor implements AttributeDescriptorProvider<${attribute.type}> {

          private final Console console = Environment.this.console
          private final AttributeDescriptor<${attribute.type}> desc = (AttributeDescriptor) ${entity.classname}Descriptor.this.desc.findAttribute("${attribute.origname}").get()
          private final Class<${attribute.type}> type = ${attribute.type}
          private final String name = "${attribute.name}"
          private final cz.o2.proxima.core.scheme.ValueSerializer<${attribute.type}> serializer = desc.getValueSerializer()

          def AttributeDescriptor<${attribute.type}> desc() {
            return desc
          }

          def Class<${attribute.type}> type() {
            return type
          }

          def String name() {
            return name
          }

          def cz.o2.proxima.core.scheme.ValueSerializer<${attribute.type}> serializer() {
            return serializer
          }

          def Stream<StreamElement> stream() {
            return stream(false);
          }

          def Stream<StreamElement> stream(boolean eventTime) {
            return console.getStream(
                desc,
                Position.NEWEST,
                false,
                eventTime);
          }

          def WindowedStream<StreamElement> streamFromOldest() {
            return streamFromOldest(true)
          }

          def WindowedStream<StreamElement> streamFromOldest(boolean stopAtCurrent) {
            return console.getStream(
                desc,
                Position.OLDEST,
                stopAtCurrent,
                true).windowAll();
          }

          def WindowedStream<StreamElement> batchSnapshot() {
            return console.getBatchSnapshot(desc);
          }

          def WindowedStream<StreamElement> batchSnapshot(long start, long end) {
            return console.getBatchSnapshot(desc, start, end);
          }

          def WindowedStream<StreamElement> batchUpdates() {
            return console.getBatchUpdates(Long.MIN_VALUE, Long.MAX_VALUE, this);
          }

          def WindowedStream<StreamElement> batchUpdates(long startStamp) {
            return console.getBatchUpdates(startStamp, Long.MAX_VALUE, this);
          }

          def WindowedStream<StreamElement> batchUpdates(long startStamp, long endStamp) {
            return console.getBatchUpdates(startStamp, endStamp, this);
          }

          <#if attribute.wildcard>
            def KeyValue<${attribute.type}> get(String key, String attribute) {
              return ${entity.classname}Descriptor.this.reader.get(key, desc.toAttributePrefix() + attribute)
            }
            def List<KeyValue<${attribute.type}>> list(String key) {
              return ${entity.classname}Descriptor.this.reader.list(key, desc.toAttributePrefix())
            }
            def List<KeyValue<${attribute.type}>> list(String key, String start, int limit) {
              return ${entity.classname}Descriptor.this.reader.list(key, name, start, limit)
            }
            def List<KeyValue<${attribute.type}>> list(String key, String start) {
              return ${entity.classname}Descriptor.this.reader.list(key, name, start)
            }
            def List<KeyValue<${attribute.type}>> listPrefix(String key, String prefix) {
              return ${entity.classname}Descriptor.this.reader.list(key, desc.toAttributePrefix() + prefix)
            }
            def void put(String key, String attribute, String jsonValue) {
              console.put(${entity.classname}Descriptor.this.desc, desc, key, desc.toAttributePrefix() + attribute, jsonValue)
            }
            def void put(String key, String attribute, long stamp, String jsonValue) {
              console.put(${entity.classname}Descriptor.this.desc, desc, key, desc.toAttributePrefix() + attribute, stamp, jsonValue)
            }
            def void delete(String key, String attribute) {
              console.delete(${entity.classname}Descriptor.this.desc, desc, key, desc.toAttributePrefix() + attribute)
            }
            def void delete(String key, String attribute, long stamp) {
              console.delete(${entity.classname}Descriptor.this.desc, desc, key, desc.toAttributePrefix() + attribute, stamp)
            }
            def void deleteAll(String key) {
              console.delete(${entity.classname}Descriptor.this.desc, desc, key, desc.getName())
            }
            def void deleteAll(String key, long stamp) {
              console.delete(${entity.classname}Descriptor.this.desc, desc, key, desc.getName(), stamp)
            }
          <#else>
            def KeyValue<${attribute.type}> get(String key) {
              return ${entity.classname}Descriptor.this.reader.get(key, name)
            }
            def void put(String key, String jsonValue) {
              console.put(${entity.classname}Descriptor.this.desc, desc, key, desc.getName(), jsonValue)
            }
            def void put(String key, long stamp, String jsonValue) {
              console.put(${entity.classname}Descriptor.this.desc, desc, key, desc.getName(), stamp, jsonValue)
            }
            def void delete(String key) {
              console.delete(${entity.classname}Descriptor.this.desc, desc, key, desc.getName())
            }
            def void delete(String key, long stamp) {
              console.delete(${entity.classname}Descriptor.this.desc, desc, key, desc.getName(), stamp)
            }
          </#if>

        }

        public final ${attribute.classname}Descriptor ${attribute.fieldname} = new ${attribute.classname}Descriptor()

      </#list>

      def void listKeys(java.util.function.Consumer<Pair<RandomOffset, String>> consumer) {
        reader.listKeys(consumer)
      }

      def List<cz.o2.proxima.core.util.Pair<cz.o2.proxima.direct.core.randomaccess.RandomOffset, String>> listKeys(
          String offset, int limit) {
        return reader.listKeys(offset, limit)
      }

    }


    ${entity.classname}Descriptor ${entity.name} = new ${entity.classname}Descriptor()


  </#list>

  def Repository getRepo() {
    return console.getRepo()
  }

  def DirectDataOperator getDirect() {
    return console.getDirect() \
        .orElseThrow({ new IllegalStateException("Missing direct operator") })
  }

  def Stream<StreamElement> unionStream(AttributeDescriptorProvider... descriptors) {
    return console.getUnionStream(Position.NEWEST, true, false, descriptors)
  }

  def Stream<StreamElement> unionStreamFromOldest(AttributeDescriptorProvider... descriptors) {
    return console.getUnionStream(Position.OLDEST, true, true, descriptors)
  }

  def Stream<StreamElement> unionStreamFromOldest(
      boolean stopAtCurrent, AttributeDescriptorProvider... descriptors) {

    return console.getUnionStream(Position.OLDEST, true, stopAtCurrent, descriptors)
  }

  def Stream<StreamElement> unionBatchUpdates(
      AttributeDescriptorProvider... descriptors) {

    return unionBatchUpdates(Long.MIN_VALUE, Long.MAX_VALUE, descriptors)
  }

  def Stream<StreamElement> unionBatchUpdates(
      long endTimestamp, AttributeDescriptorProvider... descriptors) {

    return unionBatchUpdates(Long.MIN_VALUE, endTimestamp, descriptors)
  }

  def Stream<StreamElement> unionBatchUpdates(
      long startStamp, long endTimestamp, AttributeDescriptorProvider... descriptors) {

    return console.getBatchUpdates(startStamp, endTimestamp, descriptors)
  }

  def <T> WindowedStream<T> impulse(Closure<T> factory) {
    return console.getImpulse(null, factory);
  }

  def <T> WindowedStream<T> impulse(String name, Closure<T> factory) {
    return console.getImpulse(name, factory);
  }

  def <T> WindowedStream<T> periodicImpulse(Closure<T> factory, long durationMs) {
    return console.getPeriodicImpulse(null, factory, durationMs);
  }

  def <T> WindowedStream<T> periodicImpulse(String name, Closure<T> factory, long durationMs) {
    return console.getPeriodicImpulse(name, factory, durationMs);
  }
}
