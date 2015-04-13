package org.ehcache.internal.tier;

import org.junit.Test;

/**
 * @author Aurelien Broszniowski
 */
public abstract class CachingTierSPITest<K, V> {

  protected abstract CachingTierFactory<K, V> getCachingTierFactory();

  @Test
  public void testGetOrComputeIfAbsent() throws Exception {
    CachingTierGetOrComputeIfAbsent<K, V> testSuite = new CachingTierGetOrComputeIfAbsent<K, V>(getCachingTierFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

}
