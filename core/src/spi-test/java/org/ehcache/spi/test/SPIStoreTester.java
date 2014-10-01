package org.ehcache.spi.test;

import org.ehcache.spi.cache.Store;

import java.util.concurrent.TimeUnit;

/**
 * Parent class for all Store tester classes.
 * It contains all common utility methods (e.g. instantiate a
 * {@link org.ehcache.spi.cache.Store.ValueHolder Store.ValueHolder}).
 * <p/>
 * @author Aurelien Broszniowski
 */

public class SPIStoreTester<K, V> extends SPITester {

  final StoreFactory<K,V> factory;

  public SPIStoreTester(final StoreFactory<K,V> factory) {
    this.factory = factory;
  }
  
}
