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

package org.ehcache.core.resilience;

import org.ehcache.core.spi.store.Store;
import org.ehcache.resilience.RethrowingStoreAccessException;
import org.ehcache.resilience.StoreAccessException;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;

import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;

/**
 *
 * @author Chris Dennis
 */
public class RobustLoaderWriterResilienceStrategy<K, V> extends AbstractResilienceStrategy<K, V> {

  private final RecoveryStore<K> store;
  private final CacheLoaderWriter<? super K, V> loaderWriter;
  private final boolean useLoaderInAtomics;

  public RobustLoaderWriterResilienceStrategy(Store<K, V> store, CacheLoaderWriter<? super K, V> loaderWriter, boolean useLoaderInAtomics) {
    this.store = new DefaultRecoveryStore<>(Objects.requireNonNull(store));
    this.loaderWriter = Objects.requireNonNull(loaderWriter);
    this.useLoaderInAtomics = useLoaderInAtomics;
  }

  @Override
  public V getFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    try {
      return loaderWriter.load(key);
    } catch (Exception e1) {
      throw new CacheLoadingException(e1);
    }
  }

  @Override
  public boolean containsKeyFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    return false;
  }

  @Override
  public void putFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    try {
      loaderWriter.write(key, value);
    } catch (Exception e1) {
      throw new CacheWritingException(e1);
    }
  }

  @Override
  public void removeFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    try {
      loaderWriter.delete(key);
    } catch(Exception e1) {
      throw new CacheWritingException(e1);
    }
  }

  @Override
  public void clearFailure(StoreAccessException e) {
    cleanup(e);
  }

  @Override
  public V putIfAbsentFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    // FIXME: This is not atomic
    // FIXME: Should I care about useLoaderInAtomics?
    try {
      V loaded = loaderWriter.load(key);
      if(loaded != null) {
        return loaded;
      }
    } catch (Exception e1) {
      throw new CacheLoadingException(e1);
    }
    try {
      loaderWriter.write(key, value);
    } catch (Exception e1) {
      throw new CacheWritingException(e1);
    }
    return null;
  }

  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e, boolean knownToBePresent) {
    cleanup(key, e);
    return knownToBePresent;
  }

  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e, CacheWritingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e, CacheLoadingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public V replaceFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    return null;
  }

  @Override
  public V replaceFailure(K key, V value, StoreAccessException e, CacheWritingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public V replaceFailure(K key, V value, StoreAccessException e, CacheLoadingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e, boolean knownToMatch) {
    cleanup(key, e);
    return knownToMatch;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e, CacheWritingException f) {
    cleanup(key, e);
    throw f;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e, CacheLoadingException f) {
    cleanup(key, e);
    throw f;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    cleanup(keys, e);

    try {
      return loaderWriter.loadAll((Iterable)keys); // FIXME: bad typing that we should fix
    } catch(BulkCacheLoadingException e1) {
      throw e1;
    } catch (Exception e1) {
      throw new CacheLoadingException(e1);
    }
  }

  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e) {
    cleanup(entries.keySet(), e);
  }

  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e, BulkCacheWritingException f) {
    cleanup(entries.keySet(), e);
    throw f;
  }

  @Override
  public Map<K, V> removeAllFailure(Iterable<? extends K> entries, StoreAccessException e) {
    cleanup(entries, e);
    return emptyMap();
  }

  @Override
  public Map<K, V> removeAllFailure(Iterable<? extends K> entries, StoreAccessException e, BulkCacheWritingException f) {
    cleanup(entries, e);
    throw f;
  }

  private void cleanup(StoreAccessException from) {
    filterException(from);
    try {
      store.obliterate();
    } catch (StoreAccessException e) {
      inconsistent(from, e);
      return;
    }
    recovered(from);
  }

  private void cleanup(Iterable<? extends K> keys, StoreAccessException from) {
    filterException(from);
    try {
      store.obliterate(keys);
    } catch (StoreAccessException e) {
      inconsistent(keys, from, e);
      return;
    }
    recovered(keys, from);
  }

  private void cleanup(K key, StoreAccessException from) {
    filterException(from);
    try {
      store.obliterate(key);
    } catch (StoreAccessException e) {
      inconsistent(key, from, e);
      return;
    }
    recovered(key, from);
  }

  @Deprecated
  void filterException(StoreAccessException cae) throws RuntimeException {
    if (cae instanceof RethrowingStoreAccessException) {
      throw ((RethrowingStoreAccessException) cae).getCause();
    }
  }

}
