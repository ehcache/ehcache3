package org.ehcache.internal.tier;

import org.ehcache.internal.store.StoreFactory;

/**
 * This should be the factory to instantiate the CachingTier but currently instantiates a Store since some of the
 * Store methods are needed for the SPI tests
 *
 * @author Aurelien Broszniowski
 */
public interface CachingTierFactory<K, V> extends StoreFactory<K, V> {

}
