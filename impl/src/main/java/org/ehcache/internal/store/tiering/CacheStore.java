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
package org.ehcache.internal.store.tiering;

import org.ehcache.Cache;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.spi.cache.Store;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Ludovic Orban
 */
public class CacheStore<K, V> implements Store<K, V> {

  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final CachingTier<K, V> cachingTier;
  private final AuthoritativeTier<K, V> authoritativeTier;

  public CacheStore(CachingTier<K, V> cachingTier, AuthoritativeTier<K, V> authoritativeTier) {
    this.cachingTier = cachingTier;
    this.authoritativeTier = authoritativeTier;
    this.cachingTier.enableStoreEventNotifications(new StoreEventListener<K, V>() {
      @Override
      public void onEviction(Cache.Entry<K, V> entry) {
        CacheStore.this.authoritativeTier.flush(entry);
      }
      @Override
      public void onExpiration(Cache.Entry<K, V> entry) {
        CacheStore.this.authoritativeTier.flush(entry);
      }
    });
  }

  private Lock readLock() {
    return rwLock.readLock();
  }

  private Lock writeLock() {
    return rwLock.writeLock();
  }

  @Override
  public ValueHolder<V> get(final K key) throws CacheAccessException {
    try {
      return cachingTier.cacheCompute(key, new NullaryFunction<ValueHolder<V>>() {
        @Override
        public ValueHolder<V> apply() {
          readLock().lock();
          try {
            return authoritativeTier.fault(key);
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          } finally {
            readLock().unlock();
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }


  @Override
  public boolean containsKey(K key) throws CacheAccessException {
    return authoritativeTier.containsKey(key);
  }

  @Override
  public void put(final K key, final V value) throws CacheAccessException {
    try {
      authoritativeTier.put(key, value);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public ValueHolder<V> putIfAbsent(K key, V value) throws CacheAccessException {
    ValueHolder<V> previous = null;
    try {
      previous = authoritativeTier.putIfAbsent(key, value);
    } finally {
      if (previous == null) {
        cachingTier.remove(key);
      }
    }
    return previous;
  }

  @Override
  public void remove(K key) throws CacheAccessException {
    try {
      authoritativeTier.remove(key);
    } finally {
      cachingTier.remove(key);
    }
  }

  @Override
  public boolean remove(K key, V value) throws CacheAccessException {
    boolean removed = true;
    readLock().lock();
    try {
      try {
        removed = authoritativeTier.remove(key, value);
        return removed;
      } finally {
        if (removed) {
          cachingTier.remove(key);
        }
      }
    } finally {
      readLock().unlock();
    }
  }

  @Override
  public ValueHolder<V> replace(K key, V value) throws CacheAccessException {
    ValueHolder<V> previous = null;
    boolean exceptionThrown = true;
    try {
      previous = authoritativeTier.replace(key, value);
      exceptionThrown = false;
    } finally {
      if (exceptionThrown || previous != null) {
        cachingTier.remove(key);
      }
    }
    return previous;
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) throws CacheAccessException {
    boolean replaced = true;
    try {
      replaced = authoritativeTier.replace(key, oldValue, newValue);
    } finally {
      if (replaced) {
        cachingTier.remove(key);
      }
    }
    return replaced;
  }

  @Override
  public void clear() throws CacheAccessException {
    writeLock().lock();
    try {
      authoritativeTier.clear();
    } finally {
      cachingTier.clear();
      writeLock().unlock();
    }
  }

  @Override
  public void destroy() throws CacheAccessException {
    writeLock().lock();
    try {
      authoritativeTier.destroy();
    } finally {
      cachingTier.destroy();
      writeLock().unlock();
    }
  }

  @Override
  public void create() throws CacheAccessException {
    cachingTier.create();
    authoritativeTier.create();
  }

  @Override
  public void close() {
    final Lock lock = writeLock();
    lock.lock();
    try {
      authoritativeTier.close();
    } finally {
      cachingTier.close();
      lock.unlock();
    }
  }

  @Override
  public void init() {
    cachingTier.init();
    authoritativeTier.init();
  }

  @Override
  public void maintenance() {
    cachingTier.maintenance();
    authoritativeTier.maintenance();
  }

  @Override
  public void enableStoreEventNotifications(StoreEventListener<K, V> listener) {
    authoritativeTier.enableStoreEventNotifications(listener);
  }

  @Override
  public void disableStoreEventNotifications() {
    authoritativeTier.disableStoreEventNotifications();
  }

  @Override
  public Iterator<Cache.Entry<K, ValueHolder<V>>> iterator() throws CacheAccessException {
    return authoritativeTier.iterator();
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction) throws CacheAccessException {
    try {
      return cachingTier.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K k, V v) {
          readLock().lock();
          try {
            ValueHolder<V> computed = authoritativeTier.compute(key, mappingFunction);
            return computed == null ? null : computed.value();
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          } finally {
            readLock().unlock();
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  static class ComputationException extends RuntimeException {
    public ComputationException(CacheAccessException cause) {
      super(cause);
    }
    public CacheAccessException getCacheAccessException() {
      return (CacheAccessException) getCause();
    }
  }

  @Override
  public ValueHolder<V> compute(final K key, final BiFunction<? super K, ? super V, ? extends V> mappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    try {
      return cachingTier.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K k, V v) {
          readLock().lock();
          try {
            ValueHolder<V> computed = authoritativeTier.compute(key, mappingFunction, replaceEqual);
            return computed == null ? null : computed.value();
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          } finally {
            readLock().unlock();
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  @Override
  public ValueHolder<V> computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) throws CacheAccessException {
    try {
      return cachingTier.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K k, V v) {
          readLock().lock();
          try {
            ValueHolder<V> computed = authoritativeTier.computeIfAbsent(key, mappingFunction);
            return computed == null ? null : computed.value();
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          } finally {
            readLock().unlock();
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) throws CacheAccessException {
    try {
      return cachingTier.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K k, V v) {
          readLock().lock();
          try {
            ValueHolder<V> computed = authoritativeTier.computeIfPresent(key, remappingFunction);
            return computed == null ? null : computed.value();
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          } finally {
            readLock().unlock();
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  @Override
  public ValueHolder<V> computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction, final NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    try {
      return cachingTier.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K k, V v) {
          readLock().lock();
          try {
            ValueHolder<V> computed = authoritativeTier.computeIfPresent(key, remappingFunction, replaceEqual);
            return computed == null ? null : computed.value();
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          } finally {
            readLock().unlock();
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
    return null;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction, NullaryFunction<Boolean> replaceEqual) throws CacheAccessException {
    return null;
  }

  @Override
  public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
    return null;
  }
}
