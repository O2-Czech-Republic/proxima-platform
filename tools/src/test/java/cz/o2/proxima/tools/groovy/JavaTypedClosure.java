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
package cz.o2.proxima.tools.groovy;

import groovy.lang.Closure;
import lombok.Getter;

/** A {@link Closure} that was created from java and is aware of its return type. */
public class JavaTypedClosure<T> extends Closure<T> {

  static <T> Closure<T> wrap(Closure<T> w, Class<? extends T> cls) {
    return new JavaTypedClosure<>(w, cls);
  }

  final Closure<T> delegate;
  @Getter final Class<? extends T> type;

  JavaTypedClosure(Closure<T> delegate, Class<? extends T> type) {
    super(delegate.getOwner(), delegate.getThisObject());
    this.delegate = delegate;
    this.type = type;
  }

  @Override
  public Closure<T> rehydrate(Object delegate, Object owner, Object thisObject) {
    return new JavaTypedClosure<>(this.delegate.rehydrate(delegate, owner, thisObject), type);
  }

  @Override
  public Closure<T> dehydrate() {
    return new JavaTypedClosure<>(this.delegate.dehydrate(), type);
  }

  @Override
  public void setDirective(int directive) {
    this.delegate.setDirective(directive);
  }

  @Override
  public int getDirective() {
    return this.delegate.getDirective();
  }

  @Override
  public Object clone() {
    return this.delegate.clone();
  }

  @Override
  public Closure<T> trampoline() {
    return new JavaTypedClosure<>(this.delegate.trampoline(), type);
  }

  @Override
  public Closure<T> trampoline(Object... args) {
    return new JavaTypedClosure<>(this.delegate.trampoline(args), type);
  }

  @Override
  public Closure<T> memoizeBetween(int protectedCacheSize, int maxCacheSize) {
    return new JavaTypedClosure<>(
        this.delegate.memoizeBetween(protectedCacheSize, maxCacheSize), type);
  }

  @Override
  public Closure<T> memoizeAtLeast(int protectedCacheSize) {
    return new JavaTypedClosure<>(this.delegate.memoizeAtLeast(protectedCacheSize), type);
  }

  @Override
  public Closure<T> memoizeAtMost(int maxCacheSize) {
    return new JavaTypedClosure<>(this.delegate.memoizeAtMost(maxCacheSize), type);
  }

  @Override
  public Closure<T> memoize() {
    return new JavaTypedClosure<>(this.delegate.memoize(), type);
  }

  @Override
  public T leftShift(Object arg) {
    return this.delegate.leftShift(arg);
  }

  @Override
  public Closure<T> leftShift(Closure other) {
    return new JavaTypedClosure<>(this.delegate.leftShift(other), type);
  }

  @Override
  public <W> Closure<W> rightShift(Closure<W> other) {
    return this.delegate.rightShift(other);
  }

  @Override
  public Closure<T> ncurry(int n, Object argument) {
    return new JavaTypedClosure<>(this.delegate.ncurry(n, argument), type);
  }

  @Override
  public Closure<T> ncurry(int n, Object... arguments) {
    return new JavaTypedClosure<>(this.delegate.ncurry(n, arguments), type);
  }

  @Override
  public Closure<T> rcurry(Object argument) {
    return new JavaTypedClosure<>(this.delegate.rcurry(argument), type);
  }

  @Override
  public Closure<T> rcurry(Object... arguments) {
    return new JavaTypedClosure<>(this.delegate.rcurry(arguments), type);
  }

  @Override
  public Closure<T> curry(Object argument) {
    return new JavaTypedClosure<>(this.delegate.curry(argument), type);
  }

  @Override
  public Closure<T> curry(Object... arguments) {
    return new JavaTypedClosure<>(this.delegate.curry(arguments), type);
  }

  @Override
  public void run() {
    this.delegate.run();
  }

  @Override
  public Closure asWritable() {
    return this.delegate.asWritable();
  }

  @Override
  public int getMaximumNumberOfParameters() {
    return this.delegate.getMaximumNumberOfParameters();
  }

  @Override
  public Class[] getParameterTypes() {
    return this.delegate.getParameterTypes();
  }

  @Override
  public void setDelegate(Object delegate) {
    this.delegate.setDelegate(delegate);
  }

  @Override
  public Object getDelegate() {
    return this.delegate.getDelegate();
  }

  @Override
  public Object getOwner() {
    return this.delegate.getOwner();
  }

  @Override
  public T call(Object arguments) {
    return this.delegate.call(arguments);
  }

  @Override
  public T call(Object... args) {
    return this.delegate.call(args);
  }

  @Override
  public T call() {
    return this.delegate.call();
  }

  @Override
  public boolean isCase(Object candidate) {
    return this.delegate.isCase(candidate);
  }

  @Override
  public void setProperty(String property, Object newValue) {
    this.delegate.setProperty(property, newValue);
  }

  @Override
  public Object getProperty(String property) {
    return this.delegate.getProperty(property);
  }

  @Override
  public Object getThisObject() {
    return this.delegate.getThisObject();
  }

  @Override
  public int getResolveStrategy() {
    return this.delegate.getResolveStrategy();
  }

  @Override
  public void setResolveStrategy(int resolveStrategy) {
    this.delegate.setResolveStrategy(resolveStrategy);
  }
}
