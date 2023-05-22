/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.scheme.proto;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import cz.o2.proxima.core.scheme.AttributeValueAccessors.StructureValue;
import cz.o2.proxima.core.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.internal.com.google.common.collect.ImmutableMap;
import cz.o2.proxima.scheme.proto.test.Scheme.Event;
import cz.o2.proxima.scheme.proto.test.Scheme.MessageWithWrappers;
import cz.o2.proxima.scheme.proto.test.Scheme.MultiLevelMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.RuleConfig;
import cz.o2.proxima.scheme.proto.test.Scheme.Status;
import cz.o2.proxima.scheme.proto.test.Scheme.Users;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.Directions;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.InnerMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.ValueSchemeMessage.SecondInnerMessage;
import cz.o2.proxima.scheme.proto.utils.ProtoUtils;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class ProtoMessageValueAccessorTest {

  @Test(timeout = 1000)
  public void testInspectMultiLevelMessageNotHanged() {
    assertNotNull(ProtoUtils.convertProtoToSchema(MultiLevelMessage.getDescriptor()));
  }

  @Test
  public void testManipulateWithSimpleMessage() {
    StructureValueAccessor<Event> accessor =
        new ProtoMessageValueAccessor<>(Event::getDefaultInstance);
    Event event =
        Event.newBuilder()
            .setGatewayId("gatewayId value")
            .setPayload(ByteString.copyFromUtf8("payload byte value"))
            .build();
    final StructureValue value = accessor.valueOf(event);
    assertEquals("gatewayId value", value.get("gatewayId"));
    assertArrayEquals("payload byte value".getBytes(StandardCharsets.UTF_8), value.get("payload"));

    Map<String, Object> createFrom =
        ImmutableMap.of(
            "gatewayId",
            "gatewayId value",
            "payload",
            "payload byte value".getBytes(StandardCharsets.UTF_8));

    final Event created = accessor.createFrom(StructureValue.of(createFrom));
    assertEquals(event, created);
  }

  @Test
  public void testManipulateWithSimpleRepeatedMessage() {
    final StructureValueAccessor<Users> accessor =
        new ProtoMessageValueAccessor<>(Users::getDefaultInstance);
    final List<String> values = Arrays.asList("user1", "user2", "user3");
    Users users = Users.newBuilder().addAllUser(values).build();

    assertEquals(values, accessor.valueOf(users).get("user"));
    final Users created =
        accessor.createFrom(StructureValue.of(Collections.singletonMap("user", values)));
    assertEquals(users, created);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testManipulateWithComplexMessage() {
    final StructureValueAccessor<ValueSchemeMessage> accessor =
        new ProtoMessageValueAccessor<>(ValueSchemeMessage::getDefaultInstance);
    final ValueSchemeMessage message =
        ValueSchemeMessage.newBuilder()
            .addAllRepeatedString(
                Arrays.asList("top level repeated string 1", "top level repeated string 2"))
            .addAllRepeatedInnerMessage(
                Arrays.asList(
                    InnerMessage.newBuilder()
                        .addAllRepeatedInnerString(
                            Arrays.asList(
                                "repeated_inner_message_1_repeated_inner_string_1",
                                "repeated_inner_message_1_repeated_inner_string_2"))
                        .setInnerEnum(Directions.LEFT)
                        .setInnerDoubleType(8)
                        .setInnerInnerMessage(
                            SecondInnerMessage.newBuilder().setInnerFloatType(3).buildPartial())
                        .build(),
                    InnerMessage.newBuilder()
                        .addAllRepeatedInnerString(
                            Arrays.asList(
                                "repeated_inner_message_2_repeated_inner_string_1",
                                "repeated_inner_message_2_repeated_inner_string_2"))
                        .setInnerEnum(Directions.RIGHT)
                        .setInnerDoubleType(88)
                        .setInnerInnerMessage(
                            SecondInnerMessage.newBuilder().setInnerFloatType(33).buildPartial())
                        .build()))
            .setInnerMessage(InnerMessage.newBuilder().setInnerEnum(Directions.RIGHT).build())
            .setStringType("top level string")
            .setBooleanType(true)
            .setLongType(20)
            .setIntType(69)
            .addRepeatedBytes(ByteString.copyFromUtf8("first"))
            .addRepeatedBytes(ByteString.copyFromUtf8("second"))
            .build();

    final Map<String, Object> value =
        new HashMap<>(
            ImmutableMap.<String, Object>builder()
                .put(
                    "repeated_string",
                    Arrays.asList("top level repeated string 1", "top level repeated string 2"))
                .put(
                    "repeated_inner_message",
                    Arrays.asList(
                        ImmutableMap.of(
                            "repeated_inner_string",
                            Arrays.asList(
                                "repeated_inner_message_1_repeated_inner_string_1",
                                "repeated_inner_message_1_repeated_inner_string_2"),
                            "inner_enum",
                            "LEFT",
                            "inner_double_type",
                            8.0,
                            "inner_inner_message",
                            ImmutableMap.of("inner_float_type", 3.0F)),
                        ImmutableMap.of(
                            "repeated_inner_string",
                            Arrays.asList(
                                "repeated_inner_message_2_repeated_inner_string_1",
                                "repeated_inner_message_2_repeated_inner_string_2"),
                            "inner_enum",
                            "RIGHT",
                            "inner_double_type",
                            88.0,
                            "inner_inner_message",
                            ImmutableMap.of("inner_float_type", 33.0F))))
                .put("inner_message", ImmutableMap.of("inner_enum", "RIGHT"))
                .put("string_type", "top level string")
                .put("boolean_type", true)
                .put("long_type", 20L)
                .put("int_type", 69)
                .put(
                    "repeated_bytes",
                    Arrays.asList(
                        "first".getBytes(StandardCharsets.UTF_8),
                        "second".getBytes(StandardCharsets.UTF_8)))
                .build());

    final StructureValue valueOf = accessor.valueOf(message);
    ValueSchemeMessage created = accessor.createFrom(StructureValue.of(value));
    assertEquals(message, created);

    // Comparing 2 HashMap can be tricky, specially with byte[]
    assertEquals(value.size(), value.size());
    assertArrayEquals(
        ((List<byte[]>) value.get("repeated_bytes")).toArray(),
        ((List<byte[]>) valueOf.get("repeated_bytes")).toArray());
    value.put("repeated_bytes", "SAME");
    valueOf.put("repeated_bytes", "SAME");
    assertTrue(value.keySet().containsAll(valueOf.keySet()));
  }

  @Test
  public void testCreateProtoWhereOptionalFieldChangedToRepeated() {
    // This situation can be easy simulate be creating object with field with scalar value

    final StructureValueAccessor<Users> accessor =
        new ProtoMessageValueAccessor<>(Users::getDefaultInstance);
    Users created =
        accessor.createFrom(StructureValue.of(Collections.singletonMap("user", "one single user")));
    log.debug("Created proto object {}", created);
    assertEquals("one single user", created.getUser(0));
  }

  @Test
  public void testCreateProtoWhereRepeatedFieldChangedToOptional() {
    // This situation can be simulate by creating object from list where last value should win.

    final StructureValueAccessor<RuleConfig> accessor =
        new ProtoMessageValueAccessor<>(RuleConfig::getDefaultInstance);

    RuleConfig created =
        accessor.createFrom(
            StructureValue.of(
                Collections.singletonMap(
                    "payload",
                    Arrays.asList(
                        "first".getBytes(StandardCharsets.UTF_8),
                        "second".getBytes(StandardCharsets.UTF_8)))));

    log.debug("Created proto object {}", created);
    assertArrayEquals(
        "second".getBytes(StandardCharsets.UTF_8), created.getPayload().toByteArray());
  }

  @Test
  public void testGetDefaultValuesForPrimitiveFields() {
    final StructureValueAccessor<Status> accessor =
        new ProtoMessageValueAccessor<>(Status::getDefaultInstance);
    Status status = Status.newBuilder().build();
    StructureValue value = accessor.valueOf(status);
    assertEquals(false, value.get("connected"));
    assertEquals(0L, (long) value.get("lastContact"));

    Status withDefaults = Status.newBuilder().setConnected(false).setLastContact(0).build();
    StructureValue withValues = accessor.valueOf(withDefaults);
    assertEquals(false, withValues.get("connected"));
    assertEquals(0L, (long) withValues.get("lastContact"));
  }

  @Test
  public void testGetDefaultValuesFromStructureField() {
    final StructureValueAccessor<ValueSchemeMessage> accessor =
        new ProtoMessageValueAccessor<>(ValueSchemeMessage::getDefaultInstance);
    StructureValue value = accessor.valueOf(ValueSchemeMessage.newBuilder().build());
    assertFalse(value.containsKey("inner_message"));
    assertTrue(value.containsKey("boolean_type"));
    assertFalse(value.containsKey("inner_message"));
    assertFalse(value.containsKey("repeated_string"));

    // Assert with added message
    value =
        accessor.valueOf(
            ValueSchemeMessage.newBuilder()
                .setInnerMessage(InnerMessage.newBuilder().setInnerDoubleType(5))
                .setIntType(5)
                .build());
    assertTrue(value.containsKey("inner_message"));
    assertEquals(5, (int) value.get("int_type"));

    // Assert with added repeated message
    value =
        accessor.valueOf(
            ValueSchemeMessage.newBuilder()
                .addAllRepeatedInnerMessage(
                    Arrays.asList(
                        InnerMessage.newBuilder().setInnerDoubleType(1).build(),
                        InnerMessage.newBuilder().setInnerDoubleType(2).build()))
                .addAllRepeatedString(Arrays.asList("first", "second"))
                .build());
    assertTrue(value.containsKey("repeated_inner_message"));
    assertEquals(2, ((List<?>) value.get("repeated_inner_message")).size());
    assertTrue(value.containsKey("repeated_string"));
    assertEquals(2, ((List<?>) value.get("repeated_string")).size());
  }

  @Test
  public void testValueOfFromMessageWithWrappers() {
    final StructureValueAccessor<MessageWithWrappers> accessor =
        new ProtoMessageValueAccessor<>(MessageWithWrappers::getDefaultInstance);
    // First assert empty protobuf
    MessageWithWrappers message = MessageWithWrappers.newBuilder().build();
    StructureValue value = accessor.valueOf(message);
    assertTrue(value.isEmpty());
    assertEquals(message, accessor.createFrom(value));
    // Now check protobuf with values
    message =
        MessageWithWrappers.newBuilder()
            .setBool(BoolValue.of(true))
            .setBytes(BytesValue.of(ByteString.copyFromUtf8("bytes")))
            .setDouble(DoubleValue.of(20))
            .setFloat(FloatValue.of(23.5F))
            .setInt32(Int32Value.of(32))
            .setInt64(Int64Value.of(64L))
            .setString(StringValue.of("hello world"))
            .setUint32(UInt32Value.of(332))
            .setUint64(UInt64Value.of(364L))
            .build();
    value = accessor.valueOf(message);
    assertFalse(value.isEmpty());
    assertEquals(true, value.get("bool"));
    assertArrayEquals("bytes".getBytes(StandardCharsets.UTF_8), value.get("bytes"));
    assertEquals(20, value.get("double"), 0.0001);
    assertEquals(23.5F, value.get("float"), 0.0001F);
    assertEquals(32, (int) value.get("int32"));
    assertEquals(64L, (long) value.get("int64"));
    assertEquals("hello world", value.get("string"));
    assertEquals(332, (int) value.get("uint32"));
    assertEquals(364L, (long) value.get("uint64"));

    MessageWithWrappers created = accessor.createFrom(value);
    assertEquals(message, created);
  }

  @Test
  public void testCreateFromForMessageWithWrappers() {
    final StructureValueAccessor<MessageWithWrappers> accessor =
        new ProtoMessageValueAccessor<>(MessageWithWrappers::getDefaultInstance);
    Map<String, Object> data =
        ImmutableMap.<String, Object>builder()
            .put("bool", true)
            .put("bytes", "bytes".getBytes(StandardCharsets.UTF_8))
            .put("double", 20)
            .put("float", 23.5F)
            .put("int32", 32)
            .put("int64", 64L)
            .put("string", "hello world")
            .put("uint32", 332)
            .put("uint64", 364L)
            .build();

    MessageWithWrappers message = accessor.createFrom(StructureValue.of(data));
    log.debug("Message {}", message);
    assertTrue(message.getBool().getValue());
    assertArrayEquals(
        "bytes".getBytes(StandardCharsets.UTF_8), message.getBytes().getValue().toByteArray());
    assertEquals(20, message.getDouble().getValue(), 0.00001);
    assertEquals(23.5F, message.getFloat().getValue(), 0.00001);
    assertEquals(32, message.getInt32().getValue());
    assertEquals(64L, message.getInt64().getValue());
    assertEquals("hello world", message.getString().getValue());
    assertEquals(332, message.getUint32().getValue());
    assertEquals(364L, message.getUint64().getValue());
  }
}
