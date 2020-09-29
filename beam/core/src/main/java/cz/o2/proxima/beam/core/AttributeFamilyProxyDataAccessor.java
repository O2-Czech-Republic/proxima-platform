/**
 * Copyright 2017-2020 O2 Czech Republic, a.s.
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

import com.google.common.base.Preconditions;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.AttributeFamilyProxyDescriptor;
import cz.o2.proxima.repository.AttributeProxyDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.storage.commitlog.Position;
import cz.o2.proxima.transform.ProxyTransform;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.values.PCollection;

/** {@link DataAccessor} for {@link AttributeFamilyProxyDescriptor}. */
@Slf4j
public class AttributeFamilyProxyDataAccessor implements DataAccessor {

  private static final long serialVersionUID = 1L;

  public static AttributeFamilyProxyDataAccessor of(
      AttributeFamilyProxyDescriptor proxy, DataAccessor readAccessor, DataAccessor writeAccessor) {

    return new AttributeFamilyProxyDataAccessor(proxy, readAccessor, writeAccessor);
  }

  private final AttributeFamilyProxyDescriptor proxy;
  private final DataAccessor readAccessor;
  private final DataAccessor writeAccessor;
  private final Map<AttributeDescriptor<?>, AttributeProxyDescriptor<?>> lookupTarget;
  private final Map<AttributeProxyDescriptor<?>, AttributeDescriptor<?>> lookupProxy;

  private AttributeFamilyProxyDataAccessor(
      AttributeFamilyProxyDescriptor proxy, DataAccessor readAccessor, DataAccessor writeAccessor) {

    this.proxy = proxy;
    this.readAccessor = readAccessor;
    this.writeAccessor = writeAccessor;
    this.lookupTarget =
        proxy
            .getAttributes()
            .stream()
            .map(AttributeDescriptor::asProxy)
            .collect(
                Collectors.toMap(AttributeProxyDescriptor::getReadTarget, Function.identity()));
    this.lookupProxy =
        proxy
            .getAttributes()
            .stream()
            .map(AttributeDescriptor::asProxy)
            .collect(
                Collectors.toMap(Function.identity(), AttributeProxyDescriptor::getReadTarget));
  }

  @Override
  public URI getUri() {
    return proxy.getStorageUri();
  }

  @Override
  public PCollection<StreamElement> createStream(
      String name,
      Pipeline pipeline,
      Position position,
      boolean stopAtCurrent,
      boolean eventTime,
      long limit) {

    return asBeamTransform(lookupTarget.values())
        .map(
            transform ->
                transform.createStream(name, pipeline, position, stopAtCurrent, eventTime, limit))
        .orElseGet(
            () ->
                applyTransform(
                    readAccessor.createStream(
                        name, pipeline, position, stopAtCurrent, eventTime, limit)));
  }

  @Override
  public PCollection<StreamElement> createBatch(
      Pipeline pipeline, List<AttributeDescriptor<?>> attrs, long startStamp, long endStamp) {

    return asBeamTransform(attrs)
        .map(transform -> transform.createBatch(pipeline, startStamp, endStamp))
        .orElseGet(
            () ->
                applyTransform(
                    readAccessor.createBatch(
                        pipeline, transformAttrs(attrs), startStamp, endStamp)));
  }

  @Override
  public PCollection<StreamElement> createStreamFromUpdates(
      Pipeline pipeline,
      List<AttributeDescriptor<?>> attrs,
      long startStamp,
      long endStamp,
      long limit) {

    return asBeamTransform(attrs)
        .map(transform -> transform.createStreamFromUpdates(pipeline, startStamp, endStamp, limit))
        .orElseGet(
            () ->
                applyTransform(
                    readAccessor.createStreamFromUpdates(
                        pipeline, transformAttrs(attrs), startStamp, endStamp, limit)));
  }

  List<AttributeDescriptor<?>> transformAttrs(List<AttributeDescriptor<?>> attrs) {
    return attrs
        .stream()
        .map(
            attr ->
                Objects.requireNonNull(
                    lookupProxy.get(attr.asProxy()),
                    () -> "Attribute " + attr.getName() + " is not present in proxy " + proxy))
        .collect(Collectors.toList());
  }

  private PCollection<StreamElement> applyTransform(PCollection<StreamElement> in) {
    return MapElements.of(in).using(this::transformSingleRead, in.getTypeDescriptor()).output();
  }

  private StreamElement transformSingleRead(StreamElement input) {
    AttributeProxyDescriptor<?> attr = lookupTarget.get(input.getAttributeDescriptor());
    if (attr != null) {
      ProxyTransform transform = attr.getReadTransform();
      String attribute = transform.asElementWise().toProxy(input.getAttribute());
      return StreamElement.upsert(
          input.getEntityDescriptor(),
          attr,
          input.getUuid(),
          input.getKey(),
          attribute,
          input.getStamp(),
          input.getValue());
    }
    log.warn(
        "Received unknown attribute {}. Letting though, but this " + "might cause other issues.",
        input.getAttributeDescriptor());
    return input;
  }

  private final Optional<BeamProxyTransform> asBeamTransform(
      Collection<? extends AttributeDescriptor<?>> attrs) {
    Set<? extends AttributeProxyDescriptor<?>> proxies =
        attrs.stream().map(AttributeDescriptor::asProxy).collect(Collectors.toSet());
    if (proxies.stream().allMatch(p -> p.getReadTransform().isContextual())) {
      List<ProxyTransform> transforms =
          proxies
              .stream()
              .map(AttributeProxyDescriptor::getReadTransform)
              .distinct()
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          transforms.size() == 1,
          "When using {} only single attribute on input is allowed, got [%s] in [%s]",
          BeamProxyTransform.class.getName(),
          proxies,
          attrs);

      Preconditions.checkArgument(
          transforms.get(0) instanceof BeamProxyTransform,
          "Do not mix multiple contextual proxies in single config, expected class [%s] got [%s]",
          BeamProxyTransform.class.getName(),
          transforms.get(0).getClass().getName());

      BeamProxyTransform beamProxy = (BeamProxyTransform) transforms.get(0);
      return Optional.of(beamProxy);
    }
    return Optional.empty();
  }
}
