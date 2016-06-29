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
package org.ehcache.core;

import static org.mockito.Mockito.mock;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.slf4j.LoggerFactory;

/**
 * @author Abhilash
 *
 */
public class EhcacheTest extends CacheTest {

  @Override
  protected InternalCache<Object, Object> getCache(Store store) {
    final CacheConfiguration<Object, Object> config = new BaseCacheConfiguration<Object, Object>(Object.class, Object.class, null,
        null, null, ResourcePoolsHelper.createHeapOnlyPools());
    CacheEventDispatcher<Object, Object> cacheEventDispatcher = mock(CacheEventDispatcher.class);
    return new Ehcache<Object, Object>(config, store, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheTest"));
  }

}
