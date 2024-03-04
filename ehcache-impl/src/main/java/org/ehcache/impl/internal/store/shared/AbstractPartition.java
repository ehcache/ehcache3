/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ehcache.impl.internal.store.shared;

import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class AbstractPartition<S> {

  private final int id;
  private final S shared;

  public AbstractPartition(int id, S shared) {
    this.id = id;
    this.shared = shared;
  }

  protected int id() {
    return id;
  }

  protected S shared() {
    return shared;
  }

  protected <T> CompositeValue<T> composite(T t) {
    return t == null ? null : new CompositeValue<>(id, t);
  }

  @SuppressWarnings("unchecked")
  protected <T> Store.ValueHolder<T> decode(Store.ValueHolder<CompositeValue<T>> value) {
    if (value instanceof MappingValueHolder<?, ?>) {
      return ((MappingValueHolder<T, ?>) value).getDelegate();
    } else {
      return value == null ? null : new MappingValueHolder<>(value, CompositeValue::getValue);
    }
  }

  protected <T> Store.ValueHolder<CompositeValue<T>> encode(Store.ValueHolder<T> valueHolder) {
    return valueHolder == null ? null : new MappingValueHolder<>(valueHolder, this::composite);
  }

  protected <T> Set<CompositeValue<T>> compositeSet(Iterable<? extends T> keys) {
    Set<CompositeValue<T>> compositeValueSet = new HashSet<>();
    keys.forEach(k -> compositeValueSet.add(composite(k)));
    return compositeValueSet;
  }

  private static class MappingValueHolder<T, U> implements Store.ValueHolder<U> {
    private final Store.ValueHolder<T> delegate;
    private final Function<T, U> mapping;

    private MappingValueHolder(Store.ValueHolder<T> valueHolder, Function<T, U> mapping) {
      this.delegate = valueHolder;
      this.mapping = mapping;
    }

    public Store.ValueHolder<T> getDelegate() {
      return delegate;
    }

    @Override
    @Nonnull
    public U get() {
      return mapping.apply(delegate.get());
    }

    @Override
    public long creationTime() {
      return delegate.creationTime();
    }

    @Override
    public long expirationTime() {
      return delegate.expirationTime();
    }

    @Override
    public boolean isExpired(long expirationTime) {
      return delegate.isExpired(expirationTime);
    }

    @Override
    public long lastAccessTime() {
      return delegate.lastAccessTime();
    }

    @Override
    public long getId() {
      return delegate.getId();
    }
  }
}
