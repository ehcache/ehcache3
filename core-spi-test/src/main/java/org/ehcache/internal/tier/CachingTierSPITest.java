package org.ehcache.internal.tier;

import org.ehcache.internal.store.StoreFactory;
import org.ehcache.internal.store.StoreSPITest;
import org.junit.Test;

/**
 * @author Aurelien Broszniowski
 */
public abstract class CachingTierSPITest<K, V> extends StoreSPITest<K, V> {

  protected abstract CachingTierFactory<K,V> getCachingTierFactory();

  @Test
  public void testGetOrComputeIfAbsent() throws Exception {
    CachingTierGetOrComputeIfAbsent<K, V> testSuite = new CachingTierGetOrComputeIfAbsent<K, V>(getCachingTierFactory());
    testSuite.runTestSuite().reportAndThrow();
  }

}
