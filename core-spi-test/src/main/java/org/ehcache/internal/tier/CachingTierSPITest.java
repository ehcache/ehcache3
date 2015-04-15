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

  @Test
  public void testCachingTierIsExpired() throws Exception {
    CachingTierIsExpired<K, V> testSuite = new CachingTierIsExpired<K, V>(getCachingTierFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testCachingTierRemove() throws Exception {
    CachingTierRemove<K, V> testSuite = new CachingTierRemove<K, V>(getCachingTierFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

  @Test
  public void testCachingTierClear() throws Exception {
    CachingTierClear<K, V> testSuite = new CachingTierClear<K, V>(getCachingTierFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

}
