package org.ehcache;

import org.ehcache.config.CacheConfiguration;

/**
 * @author Alex Snaps
 */
public interface CacheManager {

  <K, V> Cache<K, V> addCache(String cacheName, CacheConfiguration<K, V> configuration);

  <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType);

}
