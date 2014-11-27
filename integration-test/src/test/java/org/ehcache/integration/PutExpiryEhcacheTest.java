package org.ehcache.integration;

import org.ehcache.Cache;

import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class PutExpiryEhcacheTest extends ExpiryEhcacheTestBase {
  @Override
  protected void insert(Cache<Number, CharSequence> testCache, Map.Entry<Number, CharSequence>... entries) {
    for (Map.Entry<Number, CharSequence> entry : entries) {
      testCache.put(entry.getKey(), entry.getValue());
    }
  }
}
