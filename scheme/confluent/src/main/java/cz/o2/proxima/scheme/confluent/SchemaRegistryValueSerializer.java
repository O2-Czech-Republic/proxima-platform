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
package cz.o2.proxima.scheme.confluent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import cz.o2.proxima.scheme.SerializationException;
import cz.o2.proxima.scheme.ValueSerializer;
import cz.o2.proxima.scheme.avro.AvroSerializer;
import cz.o2.proxima.storage.UriUtil;
import cz.o2.proxima.util.Classpath;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.specific.SpecificRecord;

@Slf4j
class SchemaRegistryValueSerializer<M extends GenericContainer> implements ValueSerializer<M> {

  private static final long serialVersionUID = 1L;

  static final byte MAGIC_BYTE = 0x0;
  static final int SCHEMA_ID_SIZE = 4;
  private final URI schemaRegistryUri;
  private transient SchemaRegistryClient schemaRegistry = null;
  private Class<M> clazz = null;
  private String className = null;
  private Integer schemaId = null;
  private transient M defaultInstance = null;

  private transient Map<Integer, AvroSerializer<?>> serializersCache = null;

  SchemaRegistryValueSerializer(URI scheme) throws URISyntaxException {

    this.schemaRegistryUri = new URI(scheme.getSchemeSpecificPart());
  }

  @Override
  public Optional<M> deserialize(byte[] input) {
    return deserializeValue(input);
  }

  @Override
  public byte[] serialize(M value) {
    try {
      return serializeValue(value, getSchemaId());
    } catch (IOException e) {
      throw new SerializationException("Unable to serialize data.", e);
    }
  }

  @Override
  public M getDefault() {
    if (defaultInstance == null) {
      defaultInstance = Classpath.newInstance(getAvroClass());
    }
    return defaultInstance;
  }

  @Override
  public boolean isUsable() {
    try {
      return deserialize(serialize(getDefault())).isPresent();
    } catch (Exception ex) {
      log.warn(
          "Exception during (de)serialization of default value for "
              + "URI {}. Please consider making all fields optional, otherwise "
              + "you might encounter unexpected behavior.",
          schemaRegistryUri,
          ex);
    }
    try {
      return getDefault() != null;
    } catch (Exception ex) {
      log.warn("Error getting default value for URI {}", schemaRegistryUri, ex);
      return false;
    }
  }

  public String getClassName() {
    if (className == null) {
      getDataFromSchemaRegistry(false);
    }
    return className;
  }

  @SuppressWarnings("unchecked")
  private <M extends GenericContainer> byte[] serializeValue(M value, int schemaId)
      throws IOException {

    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(MAGIC_BYTE);
    out.write(ByteBuffer.allocate(SCHEMA_ID_SIZE).putInt(schemaId).array());
    AvroSerializer<M> avroSerializer =
        (AvroSerializer<M>)
            getAvroSerializersCache().computeIfAbsent(schemaId, this::createSerializer);
    avroSerializer.serialize(value, out);
    out.flush();
    return out.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private <M extends GenericContainer> Optional<M> deserializeValue(byte[] bytes) {

    try {
      ByteBuffer buffer = getByteBuffer(bytes);
      int dataSchemaId = buffer.getInt();
      int len = buffer.limit() - 1 - SCHEMA_ID_SIZE;
      int start = buffer.position() + buffer.arrayOffset();
      AvroSerializer<M> avroSerializer =
          (AvroSerializer<M>)
              getAvroSerializersCache().computeIfAbsent(dataSchemaId, this::createSerializer);
      return Optional.of(avroSerializer.deserialize(buffer, start, len));
    } catch (Exception e) {
      log.warn("Unable to deserialize payload.", e);
      return Optional.empty();
    }
  }

  private AvroSerializer<M> createSerializer(Integer schemaId) {
    Schema schema;
    try {
      schema = getSchemaRegistry().getById(schemaId);
    } catch (Exception e) {
      throw new SerializationException("Unable to get schema with id " + schemaId + ".", e);
    }
    return new AvroSerializer<>(schema);
  }

  private ByteBuffer getByteBuffer(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    byte magic = buffer.get();
    if (magic != MAGIC_BYTE) {
      log.warn("Unknown magic byte " + magic + ".");
    }
    return buffer;
  }

  private Class<M> getAvroClass() {
    if (clazz == null) {
      getDataFromSchemaRegistry(true);
    }
    return clazz;
  }

  private int getSchemaId() {
    if (schemaId == null) {
      getDataFromSchemaRegistry(true);
    }
    return schemaId;
  }

  private Map<Integer, AvroSerializer<?>> getAvroSerializersCache() {
    if (serializersCache == null) {
      serializersCache = new ConcurrentHashMap<>();
    }
    return serializersCache;
  }

  @SuppressWarnings("unchecked")
  private void getDataFromSchemaRegistry(boolean loadClass) {
    String subject = getSchemaRegistrySubject(schemaRegistryUri);
    SchemaMetadata metadata;
    try {
      metadata = getSchemaRegistry().getLatestSchemaMetadata(subject);
    } catch (Exception e) {
      throw new SerializationException("Unable to get schema metadata.", e);
    }
    schemaId = metadata.getId();
    Schema schema = new Schema.Parser().parse(metadata.getSchema());
    className = schema.getNamespace() + "." + schema.getName();
    if (loadClass) {
      clazz = (Class<M>) Classpath.findClass(className, SpecificRecord.class);
    }
  }

  private SchemaRegistryClient getSchemaRegistry() throws URISyntaxException {
    if (schemaRegistry == null) {
      URI baseUrl =
          new URI(
              schemaRegistryUri.getScheme(),
              schemaRegistryUri.getUserInfo(),
              schemaRegistryUri.getHost(),
              schemaRegistryUri.getPort(),
              null,
              null,
              null);
      schemaRegistry = new CachedSchemaRegistryClient(baseUrl.toString(), 10);
    }
    return schemaRegistry;
  }

  @VisibleForTesting
  void setSchemaRegistry(SchemaRegistryClient client) {
    this.schemaRegistry = client;
  }

  private String getSchemaRegistrySubject(URI uri) {
    List<String> paths = UriUtil.parsePath(uri);
    Preconditions.checkArgument(!paths.isEmpty(), "Subject cannot be empty! Uri: {}!", uri);
    return paths.get(paths.size() - 1);
  }
}
