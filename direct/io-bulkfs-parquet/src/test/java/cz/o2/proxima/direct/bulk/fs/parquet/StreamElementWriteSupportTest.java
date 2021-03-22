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
package cz.o2.proxima.direct.bulk.fs.parquet;

import static org.junit.Assert.assertEquals;

import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.bulk.fs.parquet.StreamElementWriteSupport.Writer;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.AttributeValueType;
import cz.o2.proxima.scheme.SchemaDescriptors;
import cz.o2.proxima.scheme.SchemaDescriptors.ArrayTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.SchemaTypeDescriptor;
import cz.o2.proxima.scheme.SchemaDescriptors.StructureTypeDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.TestUtils;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Before;
import org.junit.Test;

public class StreamElementWriteSupportTest {

  final Repository repository =
      ConfigRepository.of(ConfigFactory.load("test-reference.conf").resolve());
  final EntityDescriptor entity = repository.getEntity("dummy");

  final MessageType schema = ParquetUtils.createMessageWithFields(Collections.emptyList());
  final TestRecordConsumer recordConsumer = new TestRecordConsumer();
  StreamElementWriteSupport writeSupport;

  @Before
  public void setup() {
    constructWriteSupport(schema);
  }

  private void constructWriteSupport(MessageType schema) {
    writeSupport = new StreamElementWriteSupport(schema, "");
    recordConsumer.clear();
    writeSupport.prepareForWrite(recordConsumer);
  }

  @Test
  public void testWriteBytes() {
    assertWritten(
        "<bytes[1]>foo</bytes[1]>",
        SchemaDescriptors.bytes(),
        "bytes",
        1,
        "foo".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testIntWriter() {
    assertWritten("<int[1]>20</int[1]>", SchemaDescriptors.integers(), "int", 1, 20);
  }

  @Test
  public void testLongWriter() {
    assertWritten("<long[2]>8</long[2]>", SchemaDescriptors.longs(), "long", 2, 8L);
  }

  @Test
  public void testStringWriter() {
    assertWritten("<string[3]>foo</string[3]>", SchemaDescriptors.strings(), "string", 3, "foo");
  }

  @Test
  public void testBooleanWriter() {
    assertWritten("<bool[1]>true</bool[1]>", SchemaDescriptors.booleans(), "bool", 1, true);
  }

  @Test
  public void testFloatWriter() {
    assertWritten("<float[8]>88.0</float[8]>", SchemaDescriptors.floats(), "float", 8, 88F);
  }

  @Test
  public void testDoubleWriter() {
    assertWritten("<double[2]>33.0</double[2]>", SchemaDescriptors.doubles(), "double", 2, 33D);
  }

  @Test
  public void testWriteEnum() {
    assertWritten(
        "<enum[2]>LEFT</enum[2]>",
        SchemaDescriptors.enums(Arrays.asList("LEFT", "RIGHT")),
        "enum",
        2,
        "LEFT");
  }

  @Test
  public void testWriteByte() {
    assertWritten(
        "<byte[2]>1</byte[2]>",
        SchemaDescriptors.primitives(AttributeValueType.BYTE),
        "byte",
        2,
        new byte[] {0x31});
  }

  @Test
  public void testWriteArrayWithPrimitiveValue() {
    final ArrayTypeDescriptor<String> descriptor =
        SchemaDescriptors.arrays(SchemaDescriptors.strings());
    @SuppressWarnings("unchecked")
    Writer<List<Object>> writer =
        (Writer<List<Object>>)
            writeSupport.createWriter(
                descriptor,
                "repeated_string",
                schema.withNewFields(
                    ParquetUtils.mapSchemaTypeToParquet(descriptor, "repeated_string")
                        .asGroupType()));

    writer.write(Arrays.asList("first", "second"));
    assertEquals(
        "<repeated_string[0]><group><list[0]>"
            + "<group><element[0]>first</element[0]></group>"
            + "<group><element[0]>second</element[0]></group>"
            + "</list[0]></group></repeated_string[0]>",
        recordConsumer.getWritten());
  }

  @Test
  public void testWriteArrayWithStructureValue() {
    final ArrayTypeDescriptor<Object> descriptor =
        SchemaDescriptors.arrays(
            SchemaDescriptors.structures(
                "structure",
                new HashMap<String, SchemaTypeDescriptor<?>>() {
                  {
                    put("field1", SchemaDescriptors.strings());
                    put("field2", SchemaDescriptors.longs());
                  }
                }));
    @SuppressWarnings("unchecked")
    Writer<List<Object>> writer =
        (Writer<List<Object>>)
            writeSupport.createWriter(
                descriptor,
                "array",
                schema.withNewFields(
                    ParquetUtils.mapSchemaTypeToParquet(descriptor, "array").asGroupType()));

    writer.write(
        Arrays.asList(
            new HashMap<String, Object>() {
              {
                put("field1", "1-field1");
                put("field2", 8L);
              }
            },
            new HashMap<String, Object>() {
              {
                put("field1", "2-field1");
                put("field2", 88L);
              }
            }));
    assertEquals(
        "<array[0]>"
            + "<group><list[0]><group>"
            + "<element[0]><group><field1[0]>1-field1</field1[0]><field2[1]>8</field2[1]></group></element[0]>"
            + "</group><group>"
            + "<element[0]><group><field1[0]>2-field1</field1[0]><field2[1]>88</field2[1]></group></element[0]>"
            + "</group></list[0]></group></array[0]>",
        recordConsumer.getWritten());
  }

  @Test
  public void testWriteSimpleStructure() {
    final StructureTypeDescriptor<Object> descriptor =
        SchemaDescriptors.structures(
            "structure",
            new HashMap<String, SchemaTypeDescriptor<?>>() {
              {
                put("field1", SchemaDescriptors.strings());
                put("field2", SchemaDescriptors.longs());
              }
            });
    final GroupType schema =
        new GroupType(
            Repetition.OPTIONAL,
            "structure",
            ParquetUtils.mapSchemaTypeToParquet(descriptor, descriptor.getName()).asGroupType());

    @SuppressWarnings("unchecked")
    Writer<Object> writer =
        (Writer<Object>) writeSupport.createWriter(descriptor, descriptor.getName(), schema);

    writer.write(
        new HashMap<String, Object>() {
          {
            put("field1", "value of field1");
            put("field2", 8L);
          }
        });

    assertEquals(
        "<structure[0]>"
            + "<group><field1[0]>value of field1</field1[0]><field2[1]>8</field2[1]></group>"
            + "</structure[0]>",
        recordConsumer.getWritten());

    // write empty map should write nothing
    recordConsumer.clear();

    writer.write(Collections.emptyMap());
    assertEquals("", recordConsumer.getWritten());

    // missing fields should be skipped
    recordConsumer.clear();
    writer.write(
        new HashMap<String, Object>() {
          {
            put("field1", "value of field1");
          }
        });

    assertEquals(
        "<structure[0]><group><field1[0]>value of field1</field1[0]></group></structure[0]>",
        recordConsumer.getWritten());
  }

  @Test
  public void testStreamElementWriter() {
    final AttributeDescriptor<byte[]> attr = entity.getAttribute("data");
    final String uuid = UUID.randomUUID().toString();
    final long now = System.currentTimeMillis();
    final MessageType testSchema =
        ParquetUtils.createParquetSchema(
            TestUtils.createTestFamily(
                entity,
                URI.create("test:///"),
                Arrays.asList(attr, entity.getAttribute("wildcard.*")),
                Collections.emptyMap()));
    constructWriteSupport(testSchema);
    Writer<StreamElement> writer = writeSupport.new StreamElementWriter(testSchema);
    writer.write(
        StreamElement.upsert(
            entity,
            attr,
            uuid,
            "key",
            attr.getName(),
            now,
            "value".getBytes(StandardCharsets.UTF_8)));

    assertEquals(
        String.format(
            "<message>"
                + "<attribute_prefix[5]>data</attribute_prefix[5]>"
                + "<data[6]>value</data[6]>"
                + "<attribute[4]>data</attribute[4]>"
                + "<uuid[1]>%s</uuid[1]>"
                + "<operation[3]>u</operation[3]>"
                + "<key[0]>key</key[0]>"
                + "<timestamp[2]>%d</timestamp[2]>"
                + "</message>",
            uuid, now),
        recordConsumer.getWritten());

    // Delete should skip attribute value
    recordConsumer.clear();
    writer.write(StreamElement.delete(entity, attr, uuid, "key", attr.getName(), now));
    assertEquals(
        String.format(
            "<message>"
                + "<attribute_prefix[5]>data</attribute_prefix[5]>"
                + "<attribute[4]>data</attribute[4]>"
                + "<uuid[1]>%s</uuid[1]>"
                + "<operation[3]>d</operation[3]>"
                + "<key[0]>key</key[0]>"
                + "<timestamp[2]>%d</timestamp[2]>"
                + "</message>",
            uuid, now),
        recordConsumer.getWritten());
    // Same for delete wildcard

    recordConsumer.clear();
    writer.write(
        StreamElement.deleteWildcard(entity, entity.getAttribute("wildcard.*"), uuid, "key", now));
    assertEquals(
        String.format(
            "<message>"
                + "<attribute_prefix[5]>wildcard.*</attribute_prefix[5]>"
                + "<attribute[4]>wildcard.*</attribute[4]>"
                + "<uuid[1]>%s</uuid[1]>"
                + "<operation[3]>dw</operation[3]>"
                + "<key[0]>key</key[0]>"
                + "<timestamp[2]>%d</timestamp[2]>"
                + "</message>",
            uuid, now),
        recordConsumer.getWritten());

    // Check upsert for wildcard

    recordConsumer.clear();
    writer.write(
        StreamElement.upsert(
            entity,
            entity.getAttribute("wildcard.*"),
            uuid,
            "key",
            "key.123",
            now,
            "value".getBytes(StandardCharsets.UTF_8)));

    assertEquals(
        String.format(
            "<message>"
                + "<attribute_prefix[5]>wildcard.*</attribute_prefix[5]>"
                + "<attribute[4]>key.123</attribute[4]>"
                + "<uuid[1]>%s</uuid[1]>"
                + "<operation[3]>u</operation[3]>"
                + "<key[0]>key</key[0]>"
                + "<timestamp[2]>%d</timestamp[2]>"
                + "<wildcard[7]>value</wildcard[7]>"
                + "</message>",
            uuid, now),
        recordConsumer.getWritten());
  }

  private <T> void assertWritten(
      String expected,
      SchemaTypeDescriptor<T> descriptor,
      String attributeName,
      int index,
      T value) {
    @SuppressWarnings("unchecked")
    final Writer<T> writer =
        (Writer<T>)
            writeSupport.createWriter(
                descriptor,
                attributeName,
                new GroupType(
                    Repetition.REQUIRED,
                    "test-schema",
                    ParquetUtils.mapSchemaTypeToParquet(descriptor, attributeName)));
    writer.setName(attributeName);
    writer.setIndex(index);
    writer.write(value);
    assertEquals(expected, recordConsumer.getWritten());
  }

  private static class TestRecordConsumer extends RecordConsumer {
    @Getter private String written = "";

    public void clear() {
      written = "";
    }

    @Override
    public void startMessage() {
      written += "<message>";
    }

    @Override
    public void endMessage() {
      written += "</message>";
    }

    @Override
    public void startField(String field, int index) {
      written += String.format("<%s[%d]>", field, index);
    }

    @Override
    public void endField(String field, int index) {
      written += String.format("</%s[%d]>", field, index);
    }

    @Override
    public void startGroup() {
      written += "<group>";
    }

    @Override
    public void endGroup() {
      written += "</group>";
    }

    @Override
    public void addInteger(int value) {
      written += value;
    }

    @Override
    public void addLong(long value) {
      written += value;
    }

    @Override
    public void addBoolean(boolean value) {
      written += value;
    }

    @Override
    public void addBinary(Binary value) {
      written += value.toStringUsingUTF8();
    }

    @Override
    public void addFloat(float value) {
      written += value;
    }

    @Override
    public void addDouble(double value) {
      written += value;
    }
  }
}
