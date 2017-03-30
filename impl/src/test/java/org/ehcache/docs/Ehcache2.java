/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.docs;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import org.junit.Test;


public class Ehcache2 {

  @Test
  public void ehcache2Expiry() throws Exception {
    // tag::CustomExpiryEhcache2[]
    int defaultCacheTTLInSeconds = 20;

    CacheManager cacheManager = initCacheManager();
    CacheConfiguration cacheConfiguration = new CacheConfiguration().name("cache")
        .maxEntriesLocalHeap(100)
        .timeToLiveSeconds(defaultCacheTTLInSeconds);   // <1>
    cacheManager.addCache(new Cache(cacheConfiguration));

    Element element = new Element(10L, "Hello");

    int ttlInSeconds = getTimeToLiveInSeconds((Long)element.getObjectKey(), (String)element.getObjectValue()); // <2>

    if (ttlInSeconds != defaultCacheTTLInSeconds) {   // <3>
      element.setTimeToLive(ttlInSeconds);
    }

    cacheManager.getCache("cache").put(element);

    System.out.println(cacheManager.getCache("cache").get(10L).getObjectValue());

    sleep(2100);  // <4>

    // Now the returned element should be null, as the mapping is expired.
    System.out.println(cacheManager.getCache("cache").get(10L));
    // end::CustomExpiryEhcache2[]
  }

  /**
   * Returns the expiry in Seconds for the given key/value pair, based on some complex logic.
   * @param key Cache Key
   * @param value Cache Value
   * @return
   */
  private int getTimeToLiveInSeconds(Long key, String value) {
    // Returns TTL of 10 seconds for keys less than 1000
    if (key < 1000) {
      return 2;
    }

    // Otherwise return 5 seconds TTL
    return 1;
  }


  /**
   * Initialize and return the cache manager.
   * @return CacheManager
   */
  private CacheManager initCacheManager() {
    CacheManager cacheManager = new CacheManager();
    return cacheManager;
  }

  private void sleep(int millisecondsToSleep) throws Exception {
    Thread.sleep(millisecondsToSleep);
  }
}
