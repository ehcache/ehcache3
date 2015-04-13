package org.ehcache.internal.tier;

import org.ehcache.spi.test.SPITester;

/**
 * Parent class for all CachingTier tester classes.
 * <p/>
 *
 * @author Aurelien Broszniowski
 */
public class CachingTierTester<K, V> extends SPITester {

  protected final CachingTierFactory<K, V> factory;

  public CachingTierTester(final CachingTierFactory<K, V> factory) {
    this.factory = factory;
  }
}
