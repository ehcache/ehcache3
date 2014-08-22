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

package org.ehcache.config;

import org.ehcache.internal.HeapResource;
import org.ehcache.spi.cache.CacheProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.junit.Test;
import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;

import java.util.Collection;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.internal.util.Matchers.hasKey;

/**
 * @author Alex Snaps
 */
public class CachingTierConfigurationBuilderTest {

  @Test
  public void testNothing() throws CacheAccessException {

    final ServiceProvider serviceProvider = new ServiceProvider();
    final CacheConfiguration<String, String> config = newCacheConfigurationBuilder()
    .buildCacheConfig(String.class, String.class);
    CacheProvider cacheProvider = serviceProvider.findService(HeapResource.class);
    Class<String> keyType = config.getKeyType();
    Class<String> valueType = config.getValueType();
    Collection<ServiceConfiguration<?>> serviceConfigs = config.getServiceConfigurations();
    ServiceConfiguration<?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration[serviceConfigs.size()]);
    final Cache<String, String> cache = cacheProvider.createCache(keyType, valueType, serviceConfigArray);

    assertThat(cache, not(hasKey("key")));
    assertThat(cache.getAndPut("key", "value"), nullValue());
    assertThat(cache, hasKey("key"));
  }
}
