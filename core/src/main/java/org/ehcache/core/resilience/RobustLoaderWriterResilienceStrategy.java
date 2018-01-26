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

import org.ehcache.core.exceptions.ExceptionFactory;
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

/**
 *
 * @author Chris Dennis
 */
public class RobustLoaderWriterResilienceStrategy<K, V> extends AbstractResilienceStrategy<K, V> {

  private final RecoveryStore<K> store;
  private final CacheLoaderWriter<? super K, V> loaderWriter;

  public RobustLoaderWriterResilienceStrategy(Store<K, V> store, CacheLoaderWriter<? super K, V> loaderWriter) {
    this.store = new DefaultRecoveryStore<>(Objects.requireNonNull(store));
    this.loaderWriter = Objects.requireNonNull(loaderWriter);
  }

  @Override
  public V getFailure(K key, StoreAccessException e) {
    try {
      return loaderWriter.load(key);
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheLoadingException(e1, e);
    } finally {
      cleanup(key, e);
    }
  }

  @Override
  public boolean containsKeyFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    return false;
  }

  @Override
  public void putFailure(K key, V value, StoreAccessException e) {
    try {
      loaderWriter.write(key, value);
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheWritingException(e1, e);
    } finally {
      cleanup(key, e);
    }
  }

  @Override
  public void removeFailure(K key, StoreAccessException e) {
    try {
      loaderWriter.delete(key);
    } catch(Exception e1) {
      throw ExceptionFactory.newCacheWritingException(e1, e);
    } finally {
      cleanup(key, e);
    }
  }

  @Override
  public void clearFailure(StoreAccessException e) {
    cleanup(e);
  }

  @Override
  public V putIfAbsentFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    // FIXME: Should I care about useLoaderInAtomics?
    try {
      V loaded = loaderWriter.load(key);
      if(loaded != null) {
        return loaded;
      }
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheLoadingException(e1, e);
    }
    try {
      loaderWriter.write(key, value);
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheWritingException(e1, e);
    }
    return null;
  }

  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);
    V loadedValue;

    try {
      loadedValue = loaderWriter.load(key);
    } catch(Exception e1) {
      throw ExceptionFactory.newCacheLoadingException(e1, e);
    }

    if (loadedValue == null) {
      return false;
    }
    if(!loadedValue.equals(value)) {
      return false;
    }

    try {
      loaderWriter.delete(key);
    } catch(Exception e1) {
      throw ExceptionFactory.newCacheWritingException(e1, e);
    }
    return true;
  }

  @Override
  public V replaceFailure(K key, V value, StoreAccessException e) {
    cleanup(key, e);

    V oldValue;
    try {
      oldValue = loaderWriter.load(key);
    } catch(Exception e1) {
      throw ExceptionFactory.newCacheLoadingException(e1, e);
    }

    if (oldValue != null) {
      try {
        loaderWriter.write(key, value);
      } catch(Exception e1) {
        throw ExceptionFactory.newCacheWritingException(e1, e);
      }
    }

    return oldValue;
  }

  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e) {
    cleanup(key, e);

    V oldValue;
    try {
      oldValue = loaderWriter.load(key);
    } catch(Exception e1) {
      throw ExceptionFactory.newCacheLoadingException(e1, e);
    }

    if (oldValue != null && oldValue.equals(value)) {
      try {
        loaderWriter.write(key, newValue);
        return true;
      } catch(Exception e1) {
        throw ExceptionFactory.newCacheWritingException(e1, e);
      }
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    cleanup(keys, e);

    try {
      return loaderWriter.loadAll((Iterable) keys); // FIXME: bad typing that we should fix
    } catch(BulkCacheLoadingException e1) {
      throw e1;
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheLoadingException(e1, e);
    }
  }

  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e) {
    cleanup(entries.keySet(), e);

    try {
      loaderWriter.writeAll(entries.entrySet()); // FIXME: bad typing that we should fix
    } catch(BulkCacheWritingException e1) {
      throw e1;
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheWritingException(e1, e);
    }
  }

  @Override
  public void removeAllFailure(Iterable<? extends K> entries, StoreAccessException e) {
    cleanup(entries, e);

    try {
      loaderWriter.deleteAll(entries);
    } catch(BulkCacheWritingException e1) {
      throw e1;
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheWritingException(e1, e);
    }
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
