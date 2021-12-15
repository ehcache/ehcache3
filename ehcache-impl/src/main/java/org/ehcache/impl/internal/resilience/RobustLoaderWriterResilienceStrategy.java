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
package org.ehcache.impl.internal.resilience;

import org.ehcache.core.exceptions.ExceptionFactory;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.StoreAccessException;

import java.util.Map;
import java.util.Objects;

/**
 * Default resilience strategy used by a {@link org.ehcache.Cache} with a {@link CacheLoaderWriter} specified. It will
 * behaves in two ways:
 * <ul>
 *   <li>Keep the loader-writer in sync. E.g. a put will write to it</li>
 *   <li>Answer by retrieving the value from the loader-writer</li>
 * <ul>
 * Note: This behavior is the most accurate one but will add load to the loader-writer backend.
 * <p>
 * It also tries to cleanup any corrupted key.
 */
public class RobustLoaderWriterResilienceStrategy<K, V> extends AbstractResilienceStrategy<K, V> {

  private final CacheLoaderWriter<? super K, V> loaderWriter;

  public RobustLoaderWriterResilienceStrategy(RecoveryStore<K> store, CacheLoaderWriter<? super K, V> loaderWriter) {
      super(store);
    this.loaderWriter = Objects.requireNonNull(loaderWriter);
  }

  /**
   * Get the value from the loader-writer.
   *
   * @param key the key being retrieved
   * @param e the triggered failure
   * @return value as loaded from the loader-writer
   */
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

  /**
   * Return false. It doesn't matter if the key is present in the backend, we consider it's not in the cache.
   *
   * @param key the key being queried
   * @param e the triggered failure
   * @return false
   */
  @Override
  public boolean containsKeyFailure(K key, StoreAccessException e) {
    cleanup(key, e);
    return false;
  }

  /**
   * Write the value to the loader-write.
   *
   * @param key the key being put
   * @param value the value being put
   * @param e the triggered failure
   */
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

  /**
   * Delete the key from the loader-writer.
   *
   * @param key the key being removed
   * @param e the triggered failure
   */
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

  /**
   * Do nothing.
   *
   * @param e the triggered failure
   */
  @Override
  public void clearFailure(StoreAccessException e) {
    cleanup(e);
  }

  /**
   * Write the value to the loader-writer if it doesn't already exist in it. Note that the load and write pair
   * is not atomic. This atomicity, if needed, should be handled by the something else.
   *
   * @param key the key being put
   * @param value the value being put
   * @param e the triggered failure
   * @return the existing value or null if the new was set
   */
  @Override
  public V putIfAbsentFailure(K key, V value, StoreAccessException e) {
    // FIXME: Should I care about useLoaderInAtomics?
    try {
      try {
        V loaded = loaderWriter.load(key);
        if (loaded != null) {
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
    } finally {
      cleanup(key, e);
    }
    return null;
  }

  /**
   * Delete the key from the loader-writer if it is found with a matching value.  Note that the load and write pair
   * is not atomic. This atomicity, if needed, should be handled by the something else.
   *
   * @param key the key being removed
   * @param value the value being removed
   * @param e the triggered failure
   * @return if the value was removed
   */
  @Override
  public boolean removeFailure(K key, V value, StoreAccessException e) {
    try {
      V loadedValue;

      try {
        loadedValue = loaderWriter.load(key);
      } catch (Exception e1) {
        throw ExceptionFactory.newCacheLoadingException(e1, e);
      }

      if (loadedValue == null) {
        return false;
      }
      if (!loadedValue.equals(value)) {
        return false;
      }

      try {
        loaderWriter.delete(key);
      } catch (Exception e1) {
        throw ExceptionFactory.newCacheWritingException(e1, e);
      }
      return true;
    } finally {
      cleanup(key, e);
    }
  }

  /**
   * Write the value to the loader-writer if the key already exists.  Note that the load and write pair
   * is not atomic. This atomicity, if needed, should be handled by the something else.
   *
   * @param key the key being replaced
   * @param value the value being replaced
   * @param e the triggered failure
   * @return the old value or null if not found
   */
  @Override
  public V replaceFailure(K key, V value, StoreAccessException e) {
    try {
      V oldValue;
      try {
        oldValue = loaderWriter.load(key);
      } catch (Exception e1) {
        throw ExceptionFactory.newCacheLoadingException(e1, e);
      }

      if (oldValue != null) {
        try {
          loaderWriter.write(key, value);
        } catch (Exception e1) {
          throw ExceptionFactory.newCacheWritingException(e1, e);
        }
      }
      return oldValue;
    } finally {
      cleanup(key, e);
    }

  }

  /**
   * Write the value to the loader-writer if the entry already exists with a matching value.  Note that the load and write pair
   * is not atomic. This atomicity, if needed, should be handled by the something else.
   *
   * @param key the key being replaced
   * @param value the expected value
   * @param newValue the replacement value
   * @param e the triggered failure
   * @return if the value was replaced
   */
  @Override
  public boolean replaceFailure(K key, V value, V newValue, StoreAccessException e) {
    try {
      V oldValue;
      try {
        oldValue = loaderWriter.load(key);
      } catch (Exception e1) {
        throw ExceptionFactory.newCacheLoadingException(e1, e);
      }

      if (oldValue != null && oldValue.equals(value)) {
        try {
          loaderWriter.write(key, newValue);
          return true;
        } catch (Exception e1) {
          throw ExceptionFactory.newCacheWritingException(e1, e);
        }
      }

      return false;
    } finally {
      cleanup(key, e);
    }
  }

  /**
   * Get all entries for the provided keys. Entries not found by the loader-writer are expected to be an entry
   * with the key and a null value.
   *
   * @param keys the keys being retrieved
   * @param e the triggered failure
   * @return a map of key-value pairs as loaded by the loader-writer
   */
  @SuppressWarnings("unchecked")
  @Override
  public Map<K, V> getAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    try {
      return loaderWriter.loadAll((Iterable) keys); // FIXME: bad typing that we should fix
    } catch(BulkCacheLoadingException e1) {
      throw e1;
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheLoadingException(e1, e);
    } finally {
      cleanup(keys, e);
    }
  }

  /**
   * Write all entries to the loader-writer.
   *
   * @param entries the entries being put
   * @param e the triggered failure
   */
  @Override
  public void putAllFailure(Map<? extends K, ? extends V> entries, StoreAccessException e) {
    try {
      loaderWriter.writeAll(entries.entrySet()); // FIXME: bad typing that we should fix
    } catch(BulkCacheWritingException e1) {
      throw e1;
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheWritingException(e1, e);
    } finally {
      cleanup(entries.keySet(), e);
    }
  }

  /**
   * Delete all keys from the loader-writer.
   *
   * @param keys the keys being removed
   * @param e the triggered failure
   */
  @Override
  public void removeAllFailure(Iterable<? extends K> keys, StoreAccessException e) {
    try {
      loaderWriter.deleteAll(keys);
    } catch(BulkCacheWritingException e1) {
      throw e1;
    } catch (Exception e1) {
      throw ExceptionFactory.newCacheWritingException(e1, e);
    } finally {
      cleanup(keys, e);
    }
  }

}
