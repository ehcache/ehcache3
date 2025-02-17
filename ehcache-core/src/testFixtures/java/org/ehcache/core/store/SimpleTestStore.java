/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.core.store;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.spi.resilience.StoreAccessException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provides a basic {@link Store} implementation for testing.
 * The contract implemented by this {@code Store} is not strictly conformant but
 * should be sufficient for {@code Ehcache} implementation testing.
 */
public class SimpleTestStore implements Store<String, String> {

  private final CacheConfigurationChangeListener cacheConfigurationChangeListener = event -> {
    // noop
  };

  private static final Supplier<Boolean> REPLACE_EQUAL_TRUE = () -> true;
  private static final Supplier<Boolean> INVOKE_WRITER_FALSE = () -> false;

  /**
   * The key:value pairs served by this {@code Store}.  This map may be empty.
   */
  private final Map<String, FakeValueHolder> entries;

  /**
   * Keys for which access results in a thrown {@code Exception}.  This set may be empty.
   */
  private final Set<String> failingKeys;

  public SimpleTestStore(final Map<String, String> entries) {
    this(entries, Collections.emptySet());
  }

  public SimpleTestStore(final Map<String, String> entries, final Set<String> failingKeys) {
    assert failingKeys != null;

    // Use of ConcurrentHashMap is required to avoid ConcurrentModificationExceptions using Iterator.remove
    this.entries = new ConcurrentHashMap<>();
    if (entries != null) {
      for (final Map.Entry<String, String> entry : entries.entrySet()) {
        this.entries.put(entry.getKey(), new FakeValueHolder(entry.getValue()));
      }
    }
    this.failingKeys = Collections.unmodifiableSet(new HashSet<>(failingKeys));
  }

  /**
   * Gets a mapping of the entries in this {@code Store}.
   *
   * @return a new, unmodifiable map of the entries in this {@code Store}.
   */
  public Map<String, String> getEntryMap() {
    Map<String, String> result = new HashMap<>(entries.size());
    entries.forEach((k, v) -> result.put(k, v.get()));
    return Collections.unmodifiableMap(result);
  }

  @Override
  public ValueHolder<String> get(final String key) throws StoreAccessException {
    this.checkFailingKey(key);
    final FakeValueHolder valueHolder = this.entries.get(key);
    if (valueHolder != null) {
      valueHolder.lastAccessTime = System.currentTimeMillis();
    }
    return valueHolder;
  }

  @Override
  public boolean containsKey(final String key) throws StoreAccessException {
    this.checkFailingKey(key);
    return this.entries.containsKey(key);
  }

  @Override
  public PutStatus put(final String key, final String value) throws StoreAccessException {
    this.checkFailingKey(key);
    FakeValueHolder toPut = new FakeValueHolder(value);
    this.entries.put(key, toPut);
    return PutStatus.PUT;
  }

  @Override
  public ValueHolder<String> putIfAbsent(final String key, final String value, Consumer<Boolean> put) throws StoreAccessException {
    this.checkFailingKey(key);
    final FakeValueHolder currentValue = this.entries.get(key);
    if (currentValue == null) {
      this.entries.put(key, new FakeValueHolder(value));
      return null;
    }
    currentValue.lastAccessTime = System.currentTimeMillis();
    return currentValue;
  }

  @Override
  public boolean remove(final String key) throws StoreAccessException {
    this.checkFailingKey(key);
    if (this.entries.remove(key) == null) {
      return false;
    }
    return true;
  }

  @Override
  public RemoveStatus remove(final String key, final String value) throws StoreAccessException {
    this.checkFailingKey(key);
    final ValueHolder<String> currentValue = this.entries.get(key);
    if (currentValue == null) {
      return RemoveStatus.KEY_MISSING;
    } else if (!currentValue.get().equals(value)) {
      return RemoveStatus.KEY_PRESENT;
    }
    this.entries.remove(key);
    return RemoveStatus.REMOVED;
  }

  @Override
  public ValueHolder<String> replace(final String key, final String value) throws StoreAccessException {
    this.checkFailingKey(key);
    final ValueHolder<String> currentValue = this.entries.get(key);
    if (currentValue != null) {
      this.entries.put(key, new FakeValueHolder(value));
    }
    return currentValue;
  }

  @Override
  public ReplaceStatus replace(final String key, final String oldValue, final String newValue) throws StoreAccessException {
    this.checkFailingKey(key);
    final ValueHolder<String> currentValue = this.entries.get(key);
    if (currentValue == null) {
      return ReplaceStatus.MISS_NOT_PRESENT;
    }
    if (!currentValue.get().equals(oldValue)) {
      return ReplaceStatus.MISS_PRESENT;
    }
    this.entries.put(key, new FakeValueHolder(newValue));
    return ReplaceStatus.HIT;
  }

  @Override
  public void clear() throws StoreAccessException {
    this.entries.clear();
  }

  @Override
  public StoreEventSource<String, String> getStoreEventSource() {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  /**
   * {@inheritDoc}
   * <p>
   * The {@code Iterator} returned by this method <b>does not</b> have a {@code remove}
   * method.  The {@code Iterator} returned by {@code FakeStore.this.entries.entrySet().iterator()}
   * must not throw {@link java.util.ConcurrentModificationException ConcurrentModification}.
   */
  @Override
  public Iterator<Cache.Entry<String, ValueHolder<String>>> iterator() {

    return new Iterator<Cache.Entry<String, ValueHolder<String>>>() {

      final java.util.Iterator<Map.Entry<String, FakeValueHolder>> iterator =
        SimpleTestStore.this.entries.entrySet().iterator();

      @Override
      public boolean hasNext() {
        return this.iterator.hasNext();
      }

      @Override
      public Cache.Entry<String, ValueHolder<String>> next() throws StoreAccessException {

        final Map.Entry<String, FakeValueHolder> cacheEntry = this.iterator.next();
        SimpleTestStore.this.checkFailingKey(cacheEntry.getKey());
        cacheEntry.getValue().lastAccessTime = System.currentTimeMillis();

        return new Cache.Entry<String, ValueHolder<String>>() {

          @Override
          public String getKey() {
            return cacheEntry.getKey();
          }

          @Override
          public ValueHolder<String> getValue() {
            return cacheEntry.getValue();
          }

        };
      }
    };
  }

  @Override
  public ValueHolder<String> getAndPut(String key, String value) throws StoreAccessException {
    this.checkFailingKey(key);
    FakeValueHolder toPut = new FakeValueHolder(value);
    return this.entries.put(key, toPut);
  }

  @Override
  public ValueHolder<String> getAndRemove(String key) throws StoreAccessException {
    this.checkFailingKey(key);
    return this.entries.remove(key);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This method is implemented as
   * <code>this.{@link Store#getAndCompute(Object, BiFunction)} (keys, mappingFunction, () -> { returns true; })</code>
   */
  @Override
  public ValueHolder<String> getAndCompute(final String key, final BiFunction<? super String, ? super String, ? extends String> mappingFunction)
    throws StoreAccessException {
    this.checkFailingKey(key);

    FakeValueHolder currentValue = this.entries.get(key);
    this.computeInternal(key, currentValue, mappingFunction, REPLACE_EQUAL_TRUE);
    return currentValue;
  }

  /**
   * Common core for the {@link Store#computeAndGet(Object, BiFunction, Supplier, Supplier)} method.
   *
   * @param key             the key of the entry to process
   * @param currentValue    the existing value, if any, for {@code key}
   * @param mappingFunction the function that will produce the value. The function will be supplied
   *                        with the key and existing value (or null if no entry exists) as parameters. The function should
   *                        return the desired new value for the entry or null to remove the entry. If the method throws
   *                        an unchecked exception the Store will not be modified (the caller will receive the exception)
   * @param replaceEqual    If the existing value in the store is {@link Object#equals(Object)} to
   *                        the value returned from the mappingFunction this function will be invoked. If this function
   *                        returns {@link Boolean#FALSE} then the existing entry in the store will not be replaced
   *                        with a new entry and the existing entry will have its access time updated
   * @return the new value associated with the key or null if none
   */
  private FakeValueHolder computeInternal(
    final String key,
    final FakeValueHolder currentValue,
    final BiFunction<? super String, ? super String, ? extends String> mappingFunction,
    final Supplier<Boolean> replaceEqual) throws StoreAccessException {

    String remappedValue;
    try {
      remappedValue = mappingFunction.apply(key, (currentValue == null ? null : currentValue.get()));
    } catch (StorePassThroughException cpte) {
      Throwable cause = cpte.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else if (cause instanceof StoreAccessException) {
        throw (StoreAccessException) cause;
      } else {
        throw new StoreAccessException(cause);
      }
    }
    FakeValueHolder newValue = (remappedValue == null ? null : new FakeValueHolder(remappedValue));
    if (newValue == null) {
      /* Remove entry from store */
      this.entries.remove(key);
    } else if (!newValue.equals(currentValue)) {
      /* New, remapped value is different */
      this.entries.put(key, newValue);
    } else {
      /* New, remapped value is the same */
      if (replaceEqual.get()) {
        /* Replace existing equal value */
        this.entries.put(key, newValue);
      } else {
        /* Update access time of current entry */
        currentValue.lastAccessTime = System.currentTimeMillis();
        newValue = currentValue;
      }
    }
    return newValue;
  }

  @Override
  public ValueHolder<String> computeAndGet(
    final String key,
    final BiFunction<? super String, ? super String, ? extends String> mappingFunction,
    final Supplier<Boolean> replaceEqual, Supplier<Boolean> invokeWriter)
    throws StoreAccessException {
    this.checkFailingKey(key);

    return this.computeInternal(key, this.entries.get(key), mappingFunction, replaceEqual);
  }

  @Override
  public ValueHolder<String> computeIfAbsent(final String key, final Function<? super String, ? extends String> mappingFunction)
    throws StoreAccessException {
    this.checkFailingKey(key);
    FakeValueHolder currentValue = this.entries.get(key);
    if (currentValue == null) {
      String newValue;
      try {
        newValue = mappingFunction.apply(key);
      } catch (StorePassThroughException cpte) {
        Throwable cause = cpte.getCause();
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        } else if (cause instanceof StoreAccessException) {
          throw (StoreAccessException) cause;
        } else {
          throw new StoreAccessException(cause);
        }
      }
      if (newValue != null) {
        final FakeValueHolder newValueHolder = new FakeValueHolder(newValue);
        this.entries.put(key, newValueHolder);
        currentValue = newValueHolder;
      }
    } else {
      currentValue.lastAccessTime = System.currentTimeMillis();
    }
    return currentValue;
  }

  /**
   * {@inheritDoc}
   * <p>
   * This method is implemented as
   * <code>this.{@link #bulkCompute(Set, Function, Supplier)
   * bulkCompute}(keys, remappingFunction, () -> { returns true; })</code>
   */
  @Override
  public Map<String, ValueHolder<String>> bulkCompute(
    final Set<? extends String> keys,
    final Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction)
    throws StoreAccessException {
    return this.bulkCompute(keys, remappingFunction, REPLACE_EQUAL_TRUE);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation calls {@link Store#computeAndGet(Object, BiFunction, Supplier, Supplier)
   * } for each key presented in {@code keys}.
   */
  @Override
  public Map<String, ValueHolder<String>> bulkCompute(
    final Set<? extends String> keys,
    final Function<Iterable<? extends Map.Entry<? extends String, ? extends String>>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> remappingFunction,
    final Supplier<Boolean> replaceEqual)
    throws StoreAccessException {

    final Map<String, ValueHolder<String>> resultMap = new LinkedHashMap<>();
    for (final String key : keys) {
      final ValueHolder<String> newValue = this.computeAndGet(key,
        (key1, oldValue) -> {
          final Map.Entry<String, String> entry = new AbstractMap.SimpleEntry<>(key1, oldValue);
          final Map.Entry<? extends String, ? extends String> remappedEntry =
            remappingFunction.apply(Collections.singletonList(entry)).iterator().next();
          return remappedEntry.getValue();
        },
        replaceEqual, INVOKE_WRITER_FALSE);

      resultMap.put(key, newValue);
    }

    return resultMap;
  }

  /**
   * {@inheritDoc}
   * <p>
   * This implementation is based, in part, on the implementation found in
   * {@code org.ehcache.internal.store.OnHeapStore}.  This implementation calls
   * {@code mappingFunction} for each key through an internal function supplied
   * to {@link #computeIfAbsent(String, Function) computeIfAbsent}.
   */
  @Override
  public Map<String, ValueHolder<String>> bulkComputeIfAbsent(final Set<? extends String> keys, final Function<Iterable<? extends String>, Iterable<? extends Map.Entry<? extends String, ? extends String>>> mappingFunction)
    throws StoreAccessException {
    final Map<String, ValueHolder<String>> resultMap = new LinkedHashMap<>();
    for (final String key : keys) {
      final ValueHolder<String> newValue = this.computeIfAbsent(key, key1 -> {
        final Map.Entry<? extends String, ? extends String> entry =
          mappingFunction.apply(Collections.singleton(key1)).iterator().next();
        return entry.getValue();
      });
      resultMap.put(key, newValue);
    }
    return resultMap;
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> configurationChangeListenerList
      = new ArrayList<>();
    configurationChangeListenerList.add(this.cacheConfigurationChangeListener);
    return configurationChangeListenerList;
  }

  private void checkFailingKey(final String key) throws StoreAccessException {
    if (this.failingKeys.contains(key)) {
      throw new StoreAccessException(String.format("Accessing failing key: %s", key));
    }
  }

  /**
   * A {@link ValueHolder} implementation for use within
   * {@link SimpleTestStore}.
   */
  private static class FakeValueHolder implements ValueHolder<String> {

    private final String value;
    private final long creationTime;
    private long lastAccessTime;

    public FakeValueHolder(final String value) {
      this.value = value;
      this.creationTime = System.currentTimeMillis();
    }

    @Override
    public String get() {
      return this.value;
    }

    @Override
    public long creationTime() {
      return creationTime;
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
      return lastAccessTime;
    }

    @Override
    public long getId() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public String toString() {
      return "FakeValueHolder{" +
        "value='" + this.value + '\'' +
        ", creationTime=" + this.creationTime +
        ", lastAccessTime=" + this.lastAccessTime +
        '}';
    }
  }
}
