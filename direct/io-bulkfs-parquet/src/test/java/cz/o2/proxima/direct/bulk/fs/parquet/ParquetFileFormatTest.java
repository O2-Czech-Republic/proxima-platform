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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import com.google.protobuf.ByteString;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.direct.bulk.AbstractFileFormatTest;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.FileSystem;
import cz.o2.proxima.direct.bulk.NamingConvention;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.fs.parquet.ParquetFileFormat.Operation;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.repository.Repository;
import cz.o2.proxima.scheme.proto.test.Scheme.Event;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.Directions;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.InnerMessage;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Slf4j
@RunWith(Parameterized.class)
public class ParquetFileFormatTest extends AbstractFileFormatTest {

  final Repository protoRepo = Repository.ofTest(ConfigFactory.load("test-proto.conf"));

  private final AttributeDescriptor<byte[]> notFromFamilyAttribute =
      AttributeDescriptor.newBuilder(repo)
          .setEntity("entity")
          .setName("another-attribute")
          .setSchemeUri(new URI("bytes:///"))
          .build();

  @Parameterized.Parameter public ParquetTestParams parquetTestParams;

  public ParquetFileFormatTest() throws URISyntaxException {}

  @Parameterized.Parameters
  public static Collection<ParquetTestParams> params() {
    return Arrays.asList(
        ParquetTestParams.builder().expectedFileSuffix("").blockSize(100).build(),
        ParquetTestParams.builder().gzip(true).expectedFileSuffix(".gz").build(),
        ParquetTestParams.builder().gzip(false).expectedFileSuffix("").build(),
        ParquetTestParams.builder()
            .compression(CompressionCodecName.GZIP.name())
            .expectedFileSuffix(".gz")
            .build(),
        ParquetTestParams.builder()
            .compression(CompressionCodecName.SNAPPY.name())
            .expectedFileSuffix(".snappy")
            .attributeNamesPrefix("test_attribute_prefix")
            .build());
  }

  private Map<String, Object> getCfg() {
    Map<String, Object> cfg =
        new HashMap<String, Object>() {
          {
            put("gzip", parquetTestParams.gzip);
          }
        };
    if (parquetTestParams.getBlockSize() > 0) {
      cfg.put(
          ParquetFileFormat.PARQUET_CONFIG_PAGE_SIZE_KEY_NAME, parquetTestParams.getBlockSize());
    }
    if (parquetTestParams.getCompression() != null) {
      cfg.put(
          ParquetFileFormat.PARQUET_CONFIG_COMPRESSION_KEY_NAME,
          parquetTestParams.getCompression());
    }
    if (parquetTestParams.getAttributeNamesPrefix() != null) {
      cfg.put(
          ParquetFileFormat.PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX,
          parquetTestParams.getAttributeNamesPrefix());
    }
    return cfg;
  }

  @Override
  protected FileFormat getFileFormat() {
    FileFormat format = new ParquetFileFormat();
    format.setup(
        TestUtils.createTestFamily(
            entity, URI.create("test:///"), Arrays.asList(attribute, wildcard), getCfg()));
    return format;
  }

  @Test
  public void testFileExtension() {
    assertEquals(
        "parquet" + parquetTestParams.getExpectedFileSuffix(), getFileFormat().fileSuffix());
    assertEquals(
        parquetTestParams.getExpectedFileSuffix(),
        ((ParquetFileFormat) getFileFormat()).parquetCompressionCodec.getExtension());
  }

  @Test
  public void testParquetWriterSettings() {
    ParquetFileFormat format = (ParquetFileFormat) getFileFormat();
    int blockSize =
        format
            .createWriterConfiguration()
            .getInt(
                ParquetFileFormat.PARQUET_CONFIG_PAGE_SIZE_KEY_NAME,
                ParquetFileFormat.PARQUET_DEFAULT_PAGE_SIZE);
    if (parquetTestParams.blockSize > 0) {
      assertEquals(parquetTestParams.blockSize, blockSize);
    } else {
      assertEquals(ParquetFileFormat.PARQUET_DEFAULT_PAGE_SIZE, blockSize);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWriteAttributeNotFromFamilyShouldThrowsException() throws IOException {
    writeElements(
        file,
        getFileFormat(),
        entity,
        Collections.singletonList(
            StreamElement.upsert(
                entity,
                notFromFamilyAttribute,
                UUID.randomUUID().toString(),
                "key",
                notFromFamilyAttribute.toAttributePrefix(),
                now,
                new byte[] {1})));
  }

  @Test
  public void testAttributesNotFromFamilyShouldBeSkipped() throws IOException {
    URL testResource =
        getClass().getClassLoader().getResource("stored-attribute-not-in-family.parquet");
    assertNotNull("Unable to read test parquet file from resources", testResource);
    File file = new File(testResource.getPath());
    Path path =
        Path.local(
            FileSystem.local(
                file,
                NamingConvention.defaultConvention(
                    Duration.ofHours(1), "prefix", getFileFormat().fileSuffix())),
            file);

    List<StreamElement> elements = readElements(path, getFileFormat(), entity);
    assertEquals(1, elements.size()); // we have 2 elements in test file where just one is valid
  }

  @Test
  public void testWriteAndReadComplexValue() throws IOException {
    final EntityDescriptor event = protoRepo.getEntity("event");
    final AttributeDescriptor<Event> dataAttribute = event.getAttribute("data");
    List<StreamElement> elements =
        Arrays.asList(
            StreamElement.upsert(
                event,
                dataAttribute,
                UUID.randomUUID().toString(),
                "key1",
                dataAttribute.getName(),
                now,
                Event.newBuilder()
                    .setGatewayId("gatewayId1")
                    .setPayload(ByteString.copyFromUtf8("payload1"))
                    .build()
                    .toByteArray()),
            StreamElement.upsert(
                event,
                dataAttribute,
                UUID.randomUUID().toString(),
                "key2",
                dataAttribute.getName(),
                now,
                Event.newBuilder()
                    .setGatewayId("gatewayId2")
                    .setPayload(ByteString.copyFromUtf8("payload2"))
                    .build()
                    .toByteArray()));
    FileFormat format = new ParquetFileFormat();
    format.setup(
        TestUtils.createTestFamily(
            entity, URI.create("test:///"), Collections.singletonList(dataAttribute), getCfg()));

    assertWriteAndReadElements(format, event, elements);
  }

  @Test
  public void testWriteAndReadProtoBufComplexObject() throws IOException {
    final EntityDescriptor event = protoRepo.getEntity("event");
    final AttributeDescriptor<ValueSchemeMessage> complexAttr = event.getAttribute("complex");

    final FileFormat fileFormat = new ParquetFileFormat();
    fileFormat.setup(
        TestUtils.createTestFamily(
            event,
            URI.create("test:///"),
            Collections.singletonList(complexAttr),
            Collections.emptyMap()));

    assertWriteAndReadElements(
        fileFormat,
        event,
        Arrays.asList(
            StreamElement.upsert(
                event,
                complexAttr,
                UUID.randomUUID().toString(),
                "key1",
                complexAttr.getName(),
                now,
                complexAttr
                    .getValueSerializer()
                    .serialize(
                        ValueSchemeMessage.newBuilder()
                            .addRepeatedBytes(ByteString.copyFromUtf8("bytes1"))
                            .addRepeatedBytes(ByteString.copyFromUtf8("bytes2"))
                            .addRepeatedString("repeated_string_value_1")
                            .addRepeatedString("repeated_string_value_2")
                            .setInnerMessage(
                                InnerMessage.newBuilder()
                                    .setInnerEnum(Directions.LEFT)
                                    .setInnerDoubleType(69)
                                    .addRepeatedInnerString("bar")
                                    .addRepeatedInnerString("bar2")
                                    .build())
                            .addRepeatedInnerMessage(
                                InnerMessage.newBuilder()
                                    .addRepeatedInnerString("foo")
                                    .addRepeatedInnerString("bar")
                                    .setInnerDoubleType(33)
                                    .build())
                            .addRepeatedInnerMessage(
                                InnerMessage.newBuilder()
                                    .addRepeatedInnerString("foo2")
                                    .addRepeatedInnerString("bar2")
                                    .setInnerDoubleType(66)
                                    .build())
                            .setIntType(10)
                            .setBooleanType(true)
                            .build())),
            // Write completely empty element
            StreamElement.upsert(
                event,
                complexAttr,
                UUID.randomUUID().toString(),
                "key2",
                complexAttr.getName(),
                now,
                complexAttr
                    .getValueSerializer()
                    .serialize(ValueSchemeMessage.newBuilder().build()))));
  }

  @Test
  public void testOperation() {
    assertEquals(Operation.DELETE, Operation.of("d"));
    assertThrows(IllegalArgumentException.class, () -> Operation.of("unknown"));
  }

  @Builder
  @Value
  private static class ParquetTestParams {
    boolean gzip;
    String compression;
    String expectedFileSuffix;
    int blockSize;
    String attributeNamesPrefix;
  }
}
