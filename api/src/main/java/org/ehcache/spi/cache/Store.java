package org.ehcache.spi.cache;

import org.ehcache.internal.ServiceLocator;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public interface Store<K, V> {

  // Add Store methods here

  public interface Provider {

    <K, V> Store<K, V> createStore(Class<K> keyClazz, Class<V> valueClazz, ServiceLocator serviceProvider, ServiceConfiguration<?>... config);

    void releaseStore(Store<?, ?> resource);

  }

}
