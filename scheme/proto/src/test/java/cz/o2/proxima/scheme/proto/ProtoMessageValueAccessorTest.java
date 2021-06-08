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
package cz.o2.proxima.scheme.proto;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValue;
import cz.o2.proxima.scheme.AttributeValueAccessors.StructureValueAccessor;
import cz.o2.proxima.scheme.proto.test.Scheme.Event;
import cz.o2.proxima.scheme.proto.test.Scheme.MultiLevelMessage;
import cz.o2.proxima.scheme.proto.test.Scheme.RuleConfig;
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
        new HashMap<String, Object>() {
          {
            put("gatewayId", "gatewayId value");
            put("payload", "payload byte value".getBytes(StandardCharsets.UTF_8));
          }
        };

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
        new HashMap<String, Object>() {
          {
            put(
                "repeated_string",
                Arrays.asList("top level repeated string 1", "top level repeated string 2"));
            put(
                "repeated_inner_message",
                Arrays.asList(
                    new HashMap<String, Object>() {
                      {
                        put(
                            "repeated_inner_string",
                            Arrays.asList(
                                "repeated_inner_message_1_repeated_inner_string_1",
                                "repeated_inner_message_1_repeated_inner_string_2"));
                        put("inner_enum", "LEFT");
                        put("inner_double_type", 8.0);
                        put(
                            "inner_inner_message",
                            new HashMap<String, Object>() {
                              {
                                put("inner_float_type", 3.0F);
                              }
                            });
                      }
                    },
                    new HashMap<String, Object>() {
                      {
                        put(
                            "repeated_inner_string",
                            Arrays.asList(
                                "repeated_inner_message_2_repeated_inner_string_1",
                                "repeated_inner_message_2_repeated_inner_string_2"));
                        put("inner_enum", "RIGHT");
                        put("inner_double_type", 88.0);
                        put(
                            "inner_inner_message",
                            new HashMap<String, Object>() {
                              {
                                put("inner_float_type", 33.0F);
                              }
                            });
                      }
                    }));
            put(
                "inner_message",
                new HashMap<String, Object>() {
                  {
                    put("inner_enum", "RIGHT");
                  }
                });
            put("string_type", "top level string");
            put("boolean_type", true);
            put("long_type", 20L);
            put("int_type", 69);
            put(
                "repeated_bytes",
                Arrays.asList(
                    "first".getBytes(StandardCharsets.UTF_8),
                    "second".getBytes(StandardCharsets.UTF_8)));
          }
        };

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
    assertTrue(value.entrySet().containsAll(valueOf.entrySet()));
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
}
