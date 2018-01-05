
/*
 * AUTO GENERATED! DO NOT EDIT!
 */

import cz.o2.proxima.storage.commitlog.CommitLogReader.Position
import cz.o2.proxima.tools.groovy.Console
import cz.o2.proxima.tools.groovy.Stream
import cz.o2.proxima.tools.groovy.WindowedStream
import cz.o2.proxima.tools.io.ConsoleRandomReader

class Environment {

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
        class ${attribute.classname}Descriptor {
      
          private final Console console = Environment.this.console
          private final cz.o2.proxima.repository.AttributeDescriptor desc = ${entity.classname}Descriptor.this.desc.findAttribute("${attribute.origname}").get()
          private final Class type = ${attribute.typeclass}
          private final String name = "${attribute.name}"
          private final cz.o2.proxima.scheme.ValueSerializer serializer = desc.getValueSerializer()

          def cz.o2.proxima.repository.AttributeDescriptor desc() {
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
          def Stream<${attribute.type}> stream() {
            return console.getStream(
                ${entity.classname}Descriptor.this.desc, desc,
                Position.NEWEST,
                false);
          }
          def WindowedStream<${attribute.type}> streamFromOldest() {
            return console.getStream(
                ${entity.classname}Descriptor.this.desc, desc,
                Position.OLDEST,
                true,
                true).windowAll();
          }
          def WindowedStream<${attribute.type}> batchSnapshot() {
            return console.getBatchSnapshot(
                ${entity.classname}Descriptor.this.desc, desc);
          }
          def WindowedStream<${attribute.type}> batchSnapshot(long start, long end) {
            return console.getBatchSnapshot(
                ${entity.classname}Descriptor.this.desc, desc, start, end);
          }
          def WindowedStream<${attribute.type}> batchUpdates() {
            return console.getBatchUpdates(
                ${entity.classname}Descriptor.this.desc, desc, Long.MIN_VALUE, Long.MAX_VALUE);
          }
          def WindowedStream<${attribute.type}> batchUpdates(long startStamp) {
            return console.getBatchUpdates(
                ${entity.classname}Descriptor.this.desc, desc, startStamp, Long.MAX_VALUE);
          }
          def WindowedStream<${attribute.type}> batchUpdates(long startStamp, long endStamp) {
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
              return ${entity.classname}Descriptor.this.reader.list(key, name, name + "." + start, limit)
            }
            def List<cz.o2.proxima.storage.randomaccess.KeyValue<${attribute.type}>> list(String key, String start) {
              return ${entity.classname}Descriptor.this.reader.list(key, name, name + "." + start)
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

      def List<cz.o2.proxima.util.Pair<cz.o2.proxima.storage.randomaccess.RandomAccessReader.Offset, String>> listKeys(
          String offset, int limit) {
        return reader.listKeys(offset, limit)
      }

    }


    ${entity.classname}Descriptor ${entity.name} = new ${entity.classname}Descriptor()


  </#list>

}
