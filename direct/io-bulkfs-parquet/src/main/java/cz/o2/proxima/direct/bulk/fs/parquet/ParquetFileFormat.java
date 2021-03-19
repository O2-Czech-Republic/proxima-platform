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

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Internal;
import cz.o2.proxima.direct.bulk.FileFormat;
import cz.o2.proxima.direct.bulk.Path;
import cz.o2.proxima.direct.bulk.Reader;
import cz.o2.proxima.direct.bulk.Writer;
import cz.o2.proxima.repository.AttributeFamilyDescriptor;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

/** Bulk file format which stored data in Parquet format. */
@Internal
@Slf4j
public class ParquetFileFormat implements FileFormat {

  /** Config key specified compression used in parquet writer. */
  public static final String PARQUET_CONFIG_COMPRESSION_KEY_NAME = ParquetOutputFormat.COMPRESSION;

  /** Config key specified page and row group size in parquet writer. */
  public static final String PARQUET_CONFIG_PAGE_SIZE_KEY_NAME = ParquetOutputFormat.PAGE_SIZE;

  /**
   * Config key used for attribute prefix - useful when attribute name collides with proxima fields
   * defined as PARQUET_COLUMN_NAME_*
   */
  public static final String PARQUET_CONFIG_VALUES_PREFIX_KEY_NAME = "parquet.values.name.prefix";

  /** Name for top level message in parquet schema. */
  public static final String PARQUET_MESSAGE_NAME = "stream_element";

  /** Default page size used for parquet writer */
  public static final int PARQUET_DEFAULT_PAGE_SIZE = 8 * 1024 * 1024;

  /** Default attribute prefix. See {@link #PARQUET_CONFIG_VALUES_PREFIX_KEY_NAME} for details. */
  public static final String PARQUET_DEFAULT_VALUES_NAME_PREFIX = "";

  /** Key column name in parquet file */
  static final String PARQUET_COLUMN_NAME_KEY = "key";

  /** UUID column name in parquet file */
  static final String PARQUET_COLUMN_NAME_UUID = "uuid";

  /** Attribute column name in parquet file */
  static final String PARQUET_COLUMN_NAME_ATTRIBUTE = "attribute";

  /** Attribute prefix column name in parquet file */
  static final String PARQUET_COLUMN_NAME_ATTRIBUTE_PREFIX = "attribute_prefix";

  /** Timestamp column name in parquet file */
  static final String PARQUET_COLUMN_NAME_TIMESTAMP = "timestamp";

  /** Operation column name in parquet file. See {@link Operation} for details. */
  static final String PARQUET_COLUMN_NAME_OPERATION = "operation";

  /** Enum used for mapping proxima operation into (and from) parquet files. */
  enum Operation {
    UPSERT("u"),
    DELETE("d"),
    DELETE_WILDCARD("dw");
    @Getter private final String value;

    Operation(String operation) {
      this.value = operation;
    }

    static Operation of(String operation) {
      Preconditions.checkNotNull(operation);
      for (Operation op : values()) {
        if (op.getValue().equalsIgnoreCase(operation)) {
          return op;
        }
      }
      throw new IllegalArgumentException("Unknown operation " + operation);
    }

    static Operation fromElement(StreamElement e) {
      if (e.isDeleteWildcard()) {
        return DELETE_WILDCARD;
      } else if (e.isDelete()) {
        return DELETE;
      } else {
        return UPSERT;
      }
    }
  }

  /** Compression for parquet file */
  @Nullable transient CompressionCodecName parquetCompressionCodec;

  @Nullable private transient MessageType parquetSchema;
  @Nullable private transient AttributeFamilyDescriptor familyDescriptor;
  @Nullable private transient String attributeNamesPrefix;

  @Nullable private transient Configuration writerConfiguration;

  @Override
  public void setup(AttributeFamilyDescriptor family) {
    familyDescriptor = family;
    parquetCompressionCodec =
        CompressionCodecName.fromConf(
            Optional.ofNullable(family.getCfg().get(PARQUET_CONFIG_COMPRESSION_KEY_NAME))
                .map(Object::toString)
                .orElse(
                    Optional.ofNullable(family.getCfg().get("gzip"))
                        .filter(bool -> Boolean.parseBoolean(bool.toString()))
                        .map(bool -> CompressionCodecName.GZIP.name())
                        .orElse(null)));

    attributeNamesPrefix =
        Optional.ofNullable(family.getCfg().get(PARQUET_CONFIG_VALUES_PREFIX_KEY_NAME))
            .map(Object::toString)
            .orElse(PARQUET_DEFAULT_VALUES_NAME_PREFIX);
    log.info("Parquet schema for family {} is {}", family.getName(), getParquetSchema());
  }

  @Override
  public Reader openReader(Path path, EntityDescriptor entity) throws IOException {
    return new ProximaParquetReader(path, entity);
  }

  @Override
  public Writer openWriter(Path path, EntityDescriptor entity) throws IOException {
    log.debug(
        "Opening parquet writer for entity [{}] with path [{}] and schema: {}",
        entity.getName(),
        path,
        getParquetSchema());
    return new ProximaParquetWriter(
        path,
        getParquetSchema(),
        attributeNamesPrefix,
        parquetCompressionCodec,
        createWriterConfiguration());
  }

  Configuration createWriterConfiguration() {
    Preconditions.checkNotNull(familyDescriptor, "AttributeFamilyDescriptor is required.");
    Preconditions.checkNotNull(parquetCompressionCodec, "Compression codec must be set.");
    if (writerConfiguration == null) {
      writerConfiguration = new Configuration();
      Map<String, Object> familyConf = new HashMap<>(familyDescriptor.getCfg());
      familyConf.putIfAbsent(PARQUET_CONFIG_PAGE_SIZE_KEY_NAME, PARQUET_DEFAULT_PAGE_SIZE);
      familyConf.putIfAbsent(PARQUET_CONFIG_COMPRESSION_KEY_NAME, parquetCompressionCodec.name());

      familyConf.putIfAbsent(ParquetOutputFormat.BLOCK_SIZE, PARQUET_DEFAULT_PAGE_SIZE);
      familyConf.forEach(
          (k, v) -> {
            if (k.startsWith("parquet.")) {
              writerConfiguration.set(k, v.toString());
            }
          });
    }
    return writerConfiguration;
  }

  private MessageType getParquetSchema() {
    if (parquetSchema == null) {
      Preconditions.checkNotNull(familyDescriptor, "AttributeFamilyDescriptor is required.");
      parquetSchema = ParquetUtils.createParquetSchema(familyDescriptor);
    }
    return parquetSchema;
  }

  @Override
  public String fileSuffix() {
    Preconditions.checkNotNull(parquetCompressionCodec, "Compression codec must be set.");
    return "parquet" + parquetCompressionCodec.getExtension();
  }
}
