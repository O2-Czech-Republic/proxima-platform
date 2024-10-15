/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.util.state;

import static com.mongodb.internal.connection.tlschannel.util.Util.assertTrue;
import static org.junit.Assert.assertEquals;

import cz.o2.proxima.beam.util.state.MethodCallUtils.MethodInvoker;
import cz.o2.proxima.beam.util.state.MethodCallUtils.VoidMethodInvoker;
import cz.o2.proxima.core.functional.BiConsumer;
import cz.o2.proxima.core.functional.BiFunction;
import cz.o2.proxima.core.functional.Consumer;
import cz.o2.proxima.core.functional.Factory;
import cz.o2.proxima.core.functional.UnaryFunction;
import cz.o2.proxima.core.util.ExceptionUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import net.bytebuddy.ByteBuddy;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.StateBinder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.junit.Test;

public class MethodCallUtilsTest {

  @Test
  public void testBinders() {
    AtomicReference<BiFunction<Object, byte[], Iterable<StateValue>>> tmp = new AtomicReference<>();
    AtomicReference<BiConsumer<Object, StateValue>> tmp2 = new AtomicReference<>();
    testBinder(MethodCallUtils.createStateReaderBinder(tmp));
    testBinder(MethodCallUtils.createUpdaterBinder(tmp2));
  }

  @Test
  public void testMethodInvoker()
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {

    testMethodInvokerWith(Sum::new, Integer::valueOf, int.class);
  }

  @Test
  public void testMethodInvokerLong()
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {

    testMethodInvokerWith(Sum2::new, Long::valueOf, Long.class);
  }

  @Test
  public void testMethodInvokerWithVoid()
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {

    ByteBuddy buddy = new ByteBuddy();
    SumCollect s = new SumCollect();
    Method method = s.getClass().getDeclaredMethod("apply", int.class, int.class, Consumer.class);
    List<Class<?>> generated = new ArrayList<>();
    ClassCollector collector = (cls, code) -> generated.add(cls);
    VoidMethodInvoker<SumCollect> invoker = VoidMethodInvoker.of(method, buddy, collector);
    List<Integer> list = new ArrayList<>();
    Consumer<Integer> c = list::add;
    invoker.invoke(s, new Object[] {1, 2, c});
    assertEquals(3, (int) list.get(0));

    long start = System.nanoTime();
    Consumer<Integer> ign = dummy -> {};
    for (int i = 0; i < 1_000_000; i++) {
      invoker.invoke(s, new Object[] {1, 2, ign});
    }
    long duration = System.nanoTime() - start;
    assertTrue(duration < 1_000_000_000);
    assertEquals(1, generated.size());
  }

  @Test
  public void testNonStaticSubclass()
      throws InvocationTargetException,
          NoSuchMethodException,
          InstantiationException,
          IllegalAccessException {

    List<Class<?>> generated = new ArrayList<>();
    ClassCollector collector = (cls, code) -> generated.add(cls);
    Sum s =
        new Sum() {
          final MethodInvoker<Delegate, Integer> invoker =
              MethodInvoker.of(
                  ExceptionUtils.uncheckedFactory(
                      () -> Delegate.class.getDeclaredMethod("apply", int.class, int.class)),
                  new ByteBuddy(),
                  collector);

          class Delegate {
            Integer apply(int a, int b) {
              return a + b;
            }
          }

          @Override
          public Integer apply(int a, int b) {
            return invoker.invoke(new Delegate(), new Object[] {a, b});
          }
        };
    assertEquals(3, (int) s.apply(1, 2));
    assertEquals(1, generated.size());
  }

  <T, V> void testMethodInvokerWith(
      Factory<T> instanceFactory, UnaryFunction<Integer, V> valueFactory, Class<?> paramType)
      throws NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {

    ByteBuddy buddy = new ByteBuddy();
    T s = instanceFactory.apply();
    Method method = s.getClass().getDeclaredMethod("apply", paramType, paramType);
    List<Class<?>> generated = new ArrayList<>();
    ClassCollector collector = (cls, code) -> generated.add(cls);
    MethodInvoker<T, V> invoker = MethodInvoker.of(method, buddy, collector);
    assertEquals(
        valueFactory.apply(3),
        invoker.invoke(
            instanceFactory.apply(), new Object[] {valueFactory.apply(1), valueFactory.apply(2)}));

    long start = System.nanoTime();
    for (int i = 0; i < 1_000_000; i++) {
      invoker.invoke(s, new Object[] {valueFactory.apply(i), valueFactory.apply(i)});
    }
    long duration = System.nanoTime() - start;
    assertTrue(duration < 1_000_000_000);
    assertEquals(1, generated.size());
  }

  private void testBinder(StateBinder binder) {
    List<StateSpec<?>> specs =
        Arrays.asList(
            StateSpecs.bag(),
            StateSpecs.value(),
            StateSpecs.map(),
            StateSpecs.multimap(),
            StateSpecs.combining(org.apache.beam.sdk.transforms.Sum.ofIntegers()),
            StateSpecs.orderedList(VarIntCoder.of()));
    specs.forEach(s -> testBinder(s, binder));
  }

  private void testBinder(StateSpec<?> s, StateBinder binder) {
    s.bind("dummy", binder);
  }

  public static class Sum {
    public Integer apply(int a, int b) {
      return a + b;
    }
  }

  public static class Sum2 {
    public Long apply(Long a, Long b) {
      return a + b;
    }
  }

  public static class SumCollect {
    public void apply(int a, int b, Consumer<Integer> result) {
      result.accept(a + b);
    }
  }
}
