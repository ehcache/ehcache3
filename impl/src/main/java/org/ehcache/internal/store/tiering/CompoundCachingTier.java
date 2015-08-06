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

import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.cache.tiering.LowerCachingTier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ludovic Orban
 */
public class CompoundCachingTier<K, V> implements CachingTier<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompoundCachingTier.class);

  private final CachingTier<K, V> higher;
  private final LowerCachingTier<K, V> lower;
  private volatile InvalidationListener<K, V> invalidationListener;

  public CompoundCachingTier(CachingTier<K, V> higher, final LowerCachingTier<K, V> lower) {
    this.higher = higher;
    this.lower = lower;
    this.higher.setInvalidationListener(new InvalidationListener<K, V>() {
      @Override
      public void onInvalidation(final K key, final Store.ValueHolder<V> valueHolder) {
        try {
          CompoundCachingTier.this.lower.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
            @Override
            public Store.ValueHolder<V> apply(K k) {
              return valueHolder;
            }
          });
        } catch (CacheAccessException cae) {
          notifyInvalidation(key, valueHolder);
          LOGGER.warn("Error overflowing '{}' into lower caching tier {}", key, lower, cae);
        }
      }
    });
  }

  private void notifyInvalidation(K key, Store.ValueHolder<V> p) {
    final InvalidationListener<K, V> invalidationListener = this.invalidationListener;
    if (invalidationListener != null) {
      invalidationListener.onInvalidation(key, p);
    }
  }

  static class ComputationException extends RuntimeException {
    public ComputationException(CacheAccessException cause) {
      super(cause);
    }

    public CacheAccessException getCacheAccessException() {
      return (CacheAccessException) getCause();
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
  }


  @Override
  public Store.ValueHolder<V> getOrComputeIfAbsent(K key, final Function<K, Store.ValueHolder<V>> source) throws CacheAccessException {
    try {
      return higher.getOrComputeIfAbsent(key, new Function<K, Store.ValueHolder<V>>() {
        @Override
        public Store.ValueHolder<V> apply(K k) {
          try {
            Store.ValueHolder<V> valueHolder = lower.getAndRemove(k);
            if (valueHolder != null) {
              return valueHolder;
            }

            return source.apply(k);
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          }
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  @Override
  public void invalidate(final K key) throws CacheAccessException {
    try {
      higher.invalidate(key, new NullaryFunction<K>() {
        @Override
        public K apply() {
          try {
            lower.invalidate(key);
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          }
          return null;
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  @Override
  public void invalidate(final K key, final NullaryFunction<K> function) throws CacheAccessException {
    try {
      higher.invalidate(key, new NullaryFunction<K>() {
        @Override
        public K apply() {
          try {
            lower.invalidate(key, function);
          } catch (CacheAccessException cae) {
            throw new ComputationException(cae);
          }
          return null;
        }
      });
    } catch (ComputationException ce) {
      throw ce.getCacheAccessException();
    }
  }

  @Override
  public void invalidate() throws CacheAccessException {
    try {
      higher.invalidate();
    } finally {
      lower.invalidate();
    }
  }

  @Override
  public void setInvalidationListener(InvalidationListener<K, V> invalidationListener) {
    this.invalidationListener = invalidationListener;
    lower.setInvalidationListener(invalidationListener);
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> listeners = new ArrayList<CacheConfigurationChangeListener>();
    listeners.addAll(higher.getConfigurationChangeListeners());
    listeners.addAll(lower.getConfigurationChangeListeners());
    return listeners;
  }

}
