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
package org.ehcache.integration;

import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.event.CacheEventDispatcherFactoryConfiguration;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.junit.Test;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.junit.Assert.fail;

/**
 * @author Ludovic Orban
 */
public class ThreadPoolsTest {

  @Test
  public void testNoDefaultThreadPoolFails() throws Exception {
    PooledExecutionServiceConfiguration executionServiceConfiguration = new PooledExecutionServiceConfiguration();
    executionServiceConfiguration.addPool("foo", 2, 4);
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(executionServiceConfiguration)
        .build(true);

    try {
      cacheManager.createCache("testCache",
          CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
              .build());
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    cacheManager.close();
  }

  @Test
  public void testNoDefaultThreadPoolSucceedsWithExplicitConfig() throws Exception {
    PooledExecutionServiceConfiguration executionServiceConfiguration = new PooledExecutionServiceConfiguration();
    executionServiceConfiguration.addPool("foo", 2, 4);
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(new CacheEventDispatcherFactoryConfiguration("foo"))
        .using(executionServiceConfiguration)
        .build(true);

    cacheManager.createCache("testCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
            .build());

    cacheManager.close();
  }

  @Test
  public void testDefaultThreadPoolSucceedsWithoutConfig() throws Exception {
    PooledExecutionServiceConfiguration executionServiceConfiguration = new PooledExecutionServiceConfiguration();
    executionServiceConfiguration.addDefaultPool("dflt", 2, 4);
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(executionServiceConfiguration)
        .build(true);

    cacheManager.createCache("testCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
            .build());

    cacheManager.close();
  }

  @Test
  public void testDefaultThreadPoolFailsWithExplicitConfig() throws Exception {
    PooledExecutionServiceConfiguration executionServiceConfiguration = new PooledExecutionServiceConfiguration();
    executionServiceConfiguration.addDefaultPool("dflt", 2, 4);
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(new CacheEventDispatcherFactoryConfiguration("foo"))
        .using(executionServiceConfiguration)
        .build(true);

    try {
      cacheManager.createCache("testCache",
          CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
              .build());
      fail("expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    cacheManager.close();
  }

}
