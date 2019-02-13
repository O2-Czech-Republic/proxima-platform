/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.core;

import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyProxyDescriptor;
import cz.o2.proxima.repository.AttributeProxyDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.transform.ProxyTransform;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link DataAccessor} for {@link AttributeFamilyProxyDescriptor}.
 */
@Slf4j
public class AttributeFamilyProxyDataAccessor implements DataAccessor {

  public static AttributeFamilyProxyDataAccessor of(
      AttributeFamilyProxyDescriptor proxy,
      DataAccessor readAccessor,
      DataAccessor writeAccessor) {

    return new AttributeFamilyProxyDataAccessor(proxy, readAccessor, writeAccessor);
  }

  private final AttributeFamilyProxyDescriptor proxy;
  private final DataAccessor readAccessor;
  private final DataAccessor writeAccessor;
  private final Map<AttributeDescriptor<?>, AttributeProxyDescriptor<?>> lookupRead;

  private AttributeFamilyProxyDataAccessor(
      AttributeFamilyProxyDescriptor proxy,
      DataAccessor readAccessor,
      DataAccessor writeAccessor) {

    this.proxy = proxy;
    this.readAccessor = readAccessor;
    this.writeAccessor = writeAccessor;
    this.lookupRead = proxy.getAttributes()
        .stream()
        .map(AttributeDescriptor::asProxy)
        .collect(Collectors.toMap(
            AttributeProxyDescriptor::getReadTarget, Function.identity()));
  }

  @Override
  public PCollection<StreamElement> createStream(
      String name, Pipeline pipeline, Position position,
      boolean stopAtCurrent, boolean eventTime, long limit) {

    return applyTransform(readAccessor.createStream(
        name, pipeline, position, stopAtCurrent, eventTime, limit));
  }

  @Override
  public PCollection<StreamElement> createBatch(
      Pipeline pipeline, List<AttributeDescriptor<?>> attrs,
      long startStamp, long endStamp) {

    return applyTransform(readAccessor.createBatch(
        pipeline, attrs, startStamp, endStamp));
  }

  private PCollection<StreamElement> applyTransform(PCollection<StreamElement> in) {
    return MapElements.of(in)
        .using(this::transformSingleRead, in.getTypeDescriptor())
        .output();
  }

  private StreamElement transformSingleRead(StreamElement input) {
    AttributeProxyDescriptor<?> attr = lookupRead.get(input.getAttributeDescriptor());
    if (attr != null) {
      ProxyTransform transform = attr.getReadTransform();
      String attribute = transform.toProxy(input.getAttribute());
      return StreamElement.update(
          input.getEntityDescriptor(),
          attr,
          input.getUuid(),
          input.getKey(),
          attribute,
          input.getStamp(),
          input.getValue());
    }
    log.warn(
        "Received unknown attribute {}. Letting though, but this "
            + "might cause other issues.",
        input.getAttributeDescriptor());
    return input;
  }

}
