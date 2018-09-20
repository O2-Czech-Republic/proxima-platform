
/*
 * AUTO GENERATED! DO NOT EDIT!
 */

import cz.o2.proxima.repository.Repository
import cz.o2.proxima.repository.AttributeDescriptor
import cz.o2.proxima.repository.EntityDescriptor
import cz.o2.proxima.storage.StreamElement
import cz.o2.proxima.storage.commitlog.Position
import cz.o2.proxima.tools.groovy.AttributeDescriptorProvider
import cz.o2.proxima.tools.groovy.Console
import cz.o2.proxima.tools.groovy.RepositoryProvider
import cz.o2.proxima.tools.groovy.Stream
import cz.o2.proxima.tools.groovy.WindowedStream
import cz.o2.proxima.tools.io.ConsoleRandomReader
import cz.o2.proxima.tools.io.TypedStreamElement
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing

class Environment implements RepositoryProvider {

  private final Console console = Console.get()

  <#list entities as entity>

    class ${entity.classname}Descriptor {

      private final Console console = Environment.this.console
      private final cz.o2.proxima.repository.EntityDescriptor desc = console.get().findEntityDescriptor("${entity.name}")
      private final String name = "${entity.name}"
      private final ConsoleRandomReader reader = console.getRandomAccessReader("${entity.name}")

      def cz.o2.proxima.repository.EntityDescriptor desc() {
        return desc
      }

      def String name() {
        return name
      }

      /* All attributes */
      <#list entity.attributes as attribute>
        class ${attribute.classname}Descriptor implements AttributeDescriptorProvider {
      
          private final Console console = Environment.this.console
          private final AttributeDescriptor desc = ${entity.classname}Descriptor.this.desc.findAttribute("${attribute.origname}").get()
          private final Class type = ${attribute.typeclass}
          private final String name = "${attribute.name}"
          private final cz.o2.proxima.scheme.ValueSerializer serializer = desc.getValueSerializer()

          def AttributeDescriptor desc() {
            return desc
          }

          def Class type() {
            return type
          }

          def String name() {
            return name
          }

          def cz.o2.proxima.scheme.ValueSerializer serializer() {
            return serializer
          }

          def Stream<TypedStreamElement<${attribute.type}>> stream() {
            return stream(false);
          }

          def Stream<TypedStreamElement<${attribute.type}>> stream(boolean eventTime) {
            return console.getStream(
                desc,
                Position.NEWEST,
                false,
                eventTime);
          }

          def WindowedStream<TypedStreamElement<${attribute.type}>, GlobalWindowing> streamFromOldest() {
            return streamFromOldest(true)
          }

          def WindowedStream<TypedStreamElement<${attribute.type}>, GlobalWindowing> streamFromOldest(boolean stopAtCurrent) {
            return console.getStream(
                desc,
                Position.OLDEST,
                stopAtCurrent,
                true).windowAll();
          }

          def WindowedStream<TypedStreamElement<${attribute.type}>, GlobalWindowing> batchSnapshot() {
            return console.getBatchSnapshot(
                ${entity.classname}Descriptor.this.desc, desc);
          }

          def WindowedStream<TypedStreamElement<${attribute.type}>, GlobalWindowing> batchSnapshot(long start, long end) {
            return console.getBatchSnapshot(
                ${entity.classname}Descriptor.this.desc, desc, start, end);
          }

          def WindowedStream<TypedStreamElement<${attribute.type}>, GlobalWindowing> batchUpdates() {
            return console.getBatchUpdates(
                ${entity.classname}Descriptor.this.desc, desc, Long.MIN_VALUE, Long.MAX_VALUE);
          }

          def WindowedStream<TypedStreamElement<${attribute.type}>, GlobalWindowing> batchUpdates(long startStamp) {
            return console.getBatchUpdates(
                ${entity.classname}Descriptor.this.desc, desc, startStamp, Long.MAX_VALUE);
          }

          def WindowedStream<TypedStreamElement<${attribute.type}>, GlobalWindowing> batchUpdates(long startStamp, long endStamp) {
            return console.getBatchUpdates(
                ${entity.classname}Descriptor.this.desc, desc, startStamp, endStamp);
          }

          <#if attribute.wildcard>
            def cz.o2.proxima.storage.randomaccess.KeyValue<${attribute.type}> get(String key, String attribute) {
              return ${entity.classname}Descriptor.this.reader.get(key, name + "." + attribute)
            }
            def List<cz.o2.proxima.storage.randomaccess.KeyValue<${attribute.type}>> list(String key) {
              return ${entity.classname}Descriptor.this.reader.list(key, name)
            }
            def List<cz.o2.proxima.storage.randomaccess.KeyValue<${attribute.type}>> list(String key, String start, int limit) {
              return ${entity.classname}Descriptor.this.reader.list(key, name, start, limit)
            }
            def List<cz.o2.proxima.storage.randomaccess.KeyValue<${attribute.type}>> list(String key, String start) {
              return ${entity.classname}Descriptor.this.reader.list(key, name, start)
            }
            def void put(String key, String attribute, String textFormat) {
              console.put(${entity.classname}Descriptor.this.desc, desc, key, name + "." + attribute, textFormat)
            }
            def void delete(String key, String attribute) {
              console.delete(${entity.classname}Descriptor.this.desc, desc, key, desc.toAttributePrefix() + attribute)
            }
          <#else>
            def ${attribute.type} get(String key) {
              return ${entity.classname}Descriptor.this.reader.get(key, name)?.getValue()
            }
            def void put(String key, String textFormat) {
              console.put(${entity.classname}Descriptor.this.desc, desc, key, desc.getName(), textFormat)
            }
            def void delete(String key) {
              console.delete(${entity.classname}Descriptor.this.desc, desc, key, desc.getName())
            }
          </#if>

        }

        public final ${attribute.classname}Descriptor ${attribute.fieldname} = new ${attribute.classname}Descriptor()

      </#list>

      def void listKeys(java.util.function.Consumer<String> consumer) {
        reader.listKeys(consumer)
      }

      def List<cz.o2.proxima.util.Pair<cz.o2.proxima.storage.randomaccess.RandomOffset, String>> listKeys(
          String offset, int limit) {
        return reader.listKeys(offset, limit)
      }

    }


    ${entity.classname}Descriptor ${entity.name} = new ${entity.classname}Descriptor()


  </#list>

  def Repository getRepo() {
    return console.getRepo();
  }

  def Stream<StreamElement> unionStream(AttributeDescriptorProvider... descriptors) {
    return console.getUnionStream(Position.NEWEST, true, false, descriptors);
  }

  def Stream<StreamElement> unionStreamFromOldest(AttributeDescriptorProvider... descriptors) {
    return console.getUnionStream(Position.OLDEST, true, true, descriptors);
  }

  def Stream<StreamElement> unionStreamFromOldest(boolean stopAtCurrent, AttributeDescriptorProvider... descriptors) {
    return console.getUnionStream(Position.OLDEST, true, stopAtCurrent, descriptors);
  }

}
