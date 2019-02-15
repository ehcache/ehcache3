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

package org.ehcache.jsr107;

import org.junit.After;
import org.junit.Before;

import javax.cache.Caching;

/**
 * Test making sure the CacheManagers in the singleton CachingProvider are removed.
 */
public abstract class BaseCachingProviderTest {

  protected EhcacheCachingProvider provider;

  @Before // close it before as well to be sure we are not polluted by some other test
  @After
  public void closeCachingProvider() {
    provider = (EhcacheCachingProvider) Caching.getCachingProvider();
    provider.close();
  }
}
