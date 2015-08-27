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

import org.ehcache.Ehcache;
import org.ehcache.RuntimeConfiguration;
import org.ehcache.expiry.Expirations;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.internal.util.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Alex Snaps
 */
@SuppressWarnings("deprecation")
@ServiceDependencies(Store.Provider.class)
public class CachingTierConfigurationBuilderTest {

  @Test
  public void testNothing() throws Exception {

    final ServiceLocator serviceLocator = new ServiceLocator();
    serviceLocator.loadDependenciesOf(this.getClass());
    serviceLocator.startAllServices();
    
    final CacheConfiguration<String, String> config = newCacheConfigurationBuilder().buildConfig(String.class, String.class);
    final Store.Provider service = serviceLocator.getService(Store.Provider.class);
    Collection<ServiceConfiguration<?>> serviceConfigs = config.getServiceConfigurations();
    ServiceConfiguration<?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration[serviceConfigs.size()]);
    final Store<String, String> store = service.createStore(new StoreConfigurationImpl<String, String>(config, null, null), serviceConfigArray);
    final CacheConfiguration cacheConfiguration = mock(CacheConfiguration.class);
    when(cacheConfiguration.getExpiry()).thenReturn(Expirations.noExpiration());

    @SuppressWarnings("unchecked")
    RuntimeConfiguration<String, String> runtimeConfiguration = new RuntimeConfiguration<String, String>(cacheConfiguration, null);
    final Ehcache<String, String> cache = new Ehcache<String, String>(runtimeConfiguration, store, LoggerFactory.getLogger(Ehcache.class + "-" + "CachingTierConfigurationBuilderTest"));
    cache.init();

    assertThat(cache, not(hasKey("key")));
    cache.put("key", "value");
    assertThat(cache, hasKey("key"));
  }
}
