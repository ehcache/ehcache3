package org.ehcache.integration;

import org.ehcache.Cache;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class PutAllExpiryEhcacheTest extends ExpiryEhcacheTestBase {
  @Override
  protected void insert(Cache<Number, CharSequence> testCache, Map.Entry<Number, CharSequence>... entries) {
    testCache.putAll(Arrays.asList(entries));
  }
}
