/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal.cachingtier;

import org.ehcache.AtomicCache;
import org.ehcache.spi.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;

/**
 *
 * @author Chris Dennis
 */
public class TieredAtomicCache<K, V> extends TieredCache<K, V> implements AtomicCache<K, V> {

  private final AtomicCache<K, V> authority;
  
  public TieredAtomicCache(AtomicCache<K, V> authority, Class<K> keyClazz, Class<V> valueClazz, ServiceProvider serviceProvider, ServiceConfiguration<?>... configs) {
    super(authority, keyClazz, valueClazz, serviceProvider, configs);
    this.authority = authority;
  }

  @Override
  public V replace(K key, V value) {
    boolean cleanRun = false;
    V oldValue = null;
    try {
      oldValue = authority.replace(key, value);
      cleanRun = true;
    } finally {
      if (!cleanRun) {
        cachingTier.remove(key);
      } else if (oldValue != null && !cachingTier.replace(key, oldValue, value)) {
        cachingTier.remove(key);
      }
    }
    return oldValue;
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    boolean cleanRun = false;
    boolean replaced = false;
    try {
      replaced = authority.replace(key, oldValue, newValue);
      cleanRun = true;
    } finally {
      if (!cleanRun) {
        cachingTier.remove(key);
      } else if (replaced && !cachingTier.replace(key, oldValue, newValue)) {
        cachingTier.remove(key);
      }
    }
    return replaced;
  }

  @Override
  public V putIfAbsent(K key, V value) {
    boolean cleanRun = false;
    V old = null;
    try {
      old = authority.putIfAbsent(key, value);
      cleanRun = true;
    } finally {
      if (!cleanRun) {
        cachingTier.remove(key);
      } else if (old == null && cachingTier.putIfAbsent(key, value) != null) {
        cachingTier.remove(key);
      }
    }
    return old;
  }

  @Override
  public boolean remove(K key, V value) {
    boolean cleanRun = false;
    boolean removed = false;
    try {
      removed = authority.remove(key, value);
      cleanRun = true;
    } finally {
      if (!cleanRun) {
        cachingTier.remove(key);
      } else if (removed) {
        cachingTier.remove(key);
      }
    }
    return removed;
  }
}
