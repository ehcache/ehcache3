package org.ehcache.internal.tier;

import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Aurelien Broszniowski
 */
public interface CachingTierFactory<K, V> {

  CachingTier<K, V> newCachingTier(Store.Configuration<K, V> config);

  CachingTier<K, V> newCachingTier(Store.Configuration<K, V> config, TimeSource timeSource);

  Store.ValueHolder<V> newValueHolder(V value);

  Store.Provider newProvider();

  Store.Configuration<K, V> newConfiguration(
      Class<K> keyType, Class<V> valueType, Comparable<Long> capacityConstraint,
      EvictionVeto<? super K, ? super V> evictionVeto, EvictionPrioritizer<? super K, ? super V> evictionPrioritizer);

  Store.Configuration<K, V> newConfiguration(
      Class<K> keyType, Class<V> valueType, Comparable<Long> capacityConstraint,
      EvictionVeto<? super K, ? super V> evictionVeto, EvictionPrioritizer<? super K, ? super V> evictionPrioritizer,
      Expiry<? super K, ? super V> expiry);

  Class<K> getKeyType();

  Class<V> getValueType();

  ServiceConfiguration<?>[] getServiceConfigurations();

  ServiceProvider getServiceProvider();

  K createKey(long seed);

  V createValue(long seed);
}
