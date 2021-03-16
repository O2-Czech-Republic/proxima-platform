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
package cz.o2.proxima.scheme;

import com.google.common.base.Preconditions;
import cz.o2.proxima.annotations.Experimental;
import cz.o2.proxima.functional.UnaryFunction;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

/** Classes and interfaces allows manipulation with attribute value. */
@Experimental
public class AttributeValueAccessors {

  private AttributeValueAccessors() {}

  /**
   * Accessor for manipulation with {@link
   * cz.o2.proxima.scheme.SchemaDescriptors.PrimitiveTypeDescriptor}.
   *
   * <p>Allows type conversion between T and V - See Protobuf implementation for details.
   *
   * @param <T> input type
   * @param <V> output type
   */
  public interface PrimitiveValueAccessor<T, V> extends AttributeValueAccessor<T, V> {

    /**
     * Create accessor with conversion functions.
     *
     * @param valueOfCallBack called during {@link AttributeValueAccessor#valueOf(Object)}
     * @param createFromCallback called during {@link AttributeValueAccessor#createFrom(Object)}
     * @param <T> input type
     * @param <V> output type
     * @return primitive accessor
     */
    static <T, V> PrimitiveValueAccessor<T, V> of(
        UnaryFunction<T, V> valueOfCallBack, UnaryFunction<V, T> createFromCallback) {
      return new PrimitiveValueAccessorImpl<>(valueOfCallBack, createFromCallback);
    }

    default Type getType() {
      return Type.PRIMITIVE;
    }
  }

  /**
   * Accessor for manipulation with {@link
   * cz.o2.proxima.scheme.SchemaDescriptors.ArrayTypeDescriptor}.
   *
   * @param <T> input type
   * @param <V> output type
   */
  public interface ArrayValueAccessor<T, V> extends AttributeValueAccessor<List<T>, List<V>> {

    /**
     * Create new array accessor
     *
     * @param valueAccessor array value accessor
     * @param <T> input type
     * @param <V> output type
     * @return array accessor
     */
    static <T, V> ArrayValueAccessor<T, V> of(AttributeValueAccessor<T, V> valueAccessor) {
      return new ArrayValueAccessorImpl<>(valueAccessor);
    }

    default Type getType() {
      return Type.ARRAY;
    }

    AttributeValueAccessor<T, V> getValueAccessor();

    @Override
    default List<V> valueOf(List<T> object) {
      return object
          .stream()
          .map(
              v -> {
                if (getValueAccessor().getType().equals(Type.STRUCTURE)) {
                  // In inner messages we should convert output value from StructureValue
                  @SuppressWarnings("unchecked")
                  final V structureValue =
                      (V) ((StructureValue) getValueAccessor().valueOf(v)).value();
                  return structureValue;
                }
                return getValueAccessor().valueOf(v);
              })
          .collect(Collectors.toList());
    }

    @Override
    default List<T> createFrom(List<V> object) {
      return object
          .stream()
          .map(
              v -> {
                if (getValueAccessor().getType().equals(Type.STRUCTURE)) {
                  // In inner messages we should convert input value into StructureValue
                  @SuppressWarnings("unchecked")
                  final V structValue = (V) StructureValue.of((Map<String, Object>) v);
                  return getValueAccessor().createFrom(structValue);
                } else {
                  return getValueAccessor().createFrom(v);
                }
              })
          .collect(Collectors.toList());
    }
  }

  /** Interface represents value for {@link StructureValueAccessor}. */
  public interface StructureValue extends Map<String, Object> {

    /**
     * Create structure value from map.
     *
     * @param value source map
     * @return structure value
     */
    static StructureValue of(Map<String, Object> value) {
      return new StructureValueImpl(value);
    }

    /**
     * Return map with values
     *
     * @return map where key represents fields
     */
    Map<String, Object> value();

    /**
     * Type hinted get for field
     *
     * @param name name of field
     * @param <V> returned type
     * @return value of field
     */
    <V> V get(String name);
  }

  public interface StructureValueAccessor<T> extends AttributeValueAccessor<T, StructureValue> {

    @Override
    default Type getType() {
      return Type.STRUCTURE;
    }
  }

  /** Default implementation of {@link StructureValueAccessor} delegates methods to Map. */
  public static class StructureValueImpl implements StructureValue {
    private final Map<String, Object> delegate;

    private StructureValueImpl(Map<String, Object> delegate) {
      this.delegate = delegate;
    }

    @Override
    public Map<String, Object> value() {
      return delegate;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V> V get(String name) {
      return (V) delegate.get(name);
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return this.delegate.containsValue(value);
    }

    @Override
    public Object get(Object key) {
      return delegate.get(key);
    }

    @Override
    public Object put(String key, Object value) {
      return this.delegate.put(key, value);
    }

    @Override
    public Object remove(Object key) {
      return delegate.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
      delegate.putAll(m);
    }

    @Override
    public void clear() {
      delegate.clear();
    }

    @Override
    public Set<String> keySet() {
      return delegate.keySet();
    }

    @Override
    public Collection<Object> values() {
      return delegate.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
      return delegate.entrySet();
    }
  }

  /**
   * Default implementation of {@link PrimitiveValueAccessor} which use {@link UnaryFunction} to
   * conversions.
   *
   * @param <T> input type
   * @param <V> output type
   */
  public static class PrimitiveValueAccessorImpl<T, V> implements PrimitiveValueAccessor<T, V> {

    private final UnaryFunction<T, V> valueOfConvertCallback;
    private final UnaryFunction<V, T> createFromConvertCallback;

    public PrimitiveValueAccessorImpl(
        UnaryFunction<T, V> valueOfConvertCallback, UnaryFunction<V, T> createFromConvertCallback) {
      Preconditions.checkNotNull(valueOfConvertCallback, "ValueOfConvertCallback can not be null.");
      Preconditions.checkNotNull(
          createFromConvertCallback, "CreateFromConvertCallback can not be null.");
      this.valueOfConvertCallback = valueOfConvertCallback;
      this.createFromConvertCallback = createFromConvertCallback;
    }

    @Override
    public V valueOf(T object) {
      return valueOfConvertCallback.apply(object);
    }

    @Override
    public T createFrom(V object) {
      return createFromConvertCallback.apply(object);
    }
  }

  /**
   * Default implementation of {@link ArrayValueAccessor}.
   *
   * @param <T> input type
   * @param <V> output type
   */
  public static class ArrayValueAccessorImpl<T, V> implements ArrayValueAccessor<T, V> {
    @Getter private final AttributeValueAccessor<T, V> valueAccessor;

    public ArrayValueAccessorImpl(AttributeValueAccessor<T, V> valueAccessor) {
      Preconditions.checkNotNull(valueAccessor, "ValueAccessor can not be null.");
      this.valueAccessor = valueAccessor;
    }
  }
}
