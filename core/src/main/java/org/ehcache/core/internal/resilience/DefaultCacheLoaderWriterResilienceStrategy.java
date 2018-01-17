package org.ehcache.core.internal.resilience;

import org.ehcache.core.resilience.RecoveryStore;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DefaultCacheLoaderWriterResilienceStrategy<K, V> implements CacheLoaderWriterResilienceStrategy<K, V> {

  private final RecoveryStore<K> recoveryCache;

  public DefaultCacheLoaderWriterResilienceStrategy(RecoveryStore<K> recoveryCache) {
    this.recoveryCache = recoveryCache;
  }

  @Override
  public V load(K key, Exception e) throws Exception {
    throw e;
  }

  @Override
  public Map<K, V> loadAll(Iterable<? extends K> keys, Exception e) throws Exception {
    throw e;
  }

  @Override
  public void write(K key, V value, Exception e) throws Exception {
    recoveryCache.obliterate(key);
    throw e;
  }

  @Override
  public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries, Exception e) throws Exception {
    recoveryCache.obliterate(
      StreamSupport.stream(entries.spliterator(), false)
        .map(entry -> entry.getKey())
        .collect(Collectors.toSet()));
    throw e;
  }

  @Override
  public void delete(K key, Exception e) throws Exception {
    recoveryCache.obliterate(key);
    throw e;
  }

  @Override
  public void deleteAll(Iterable<? extends K> keys, Exception e) throws Exception {
    recoveryCache.obliterate(keys);
    throw e;
  }
}
