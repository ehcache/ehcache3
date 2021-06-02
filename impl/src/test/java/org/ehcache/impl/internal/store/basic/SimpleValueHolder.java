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

package org.ehcache.impl.internal.store.basic;

import org.ehcache.core.spi.store.Store;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

/**
 * A really basic value holder that just holds a value.
 */
public class SimpleValueHolder<T> implements Store.ValueHolder<T> {

  private final T value;

  public SimpleValueHolder(T v) {
    this.value = v;
  }

  @Override
  @Nonnull
  public T get() {
    return value;
  }

  @Override
  public long creationTime() {
    return 0;
  }

  @Override
  public long expirationTime() {
    return 0;
  }

  @Override
  public boolean isExpired(long expirationTime) {
    return false;
  }

  @Override
  public long lastAccessTime() {
    return 0;
  }

  @Override
  public long getId() {
    return 0;
  }
}
