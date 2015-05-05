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
package org.ehcache.loaderwriter.writebehind;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.writebehind.WriteBehindConfiguration;
import org.ehcache.config.writebehind.WriteBehindConfigurationBuilder;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterFactory;
import org.junit.Before;

/**
 * @author Abhilash
 *
 */
public class WriteBehindEvictionTest extends AbstractWriteBehindTestBase {

  @Before
  public void setUp(){
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheLoaderWriterFactory cacheLoaderWriterFactory = mock(CacheLoaderWriterFactory.class);
    
    when(cacheLoaderWriterFactory.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);
    
    WriteBehindConfigurationBuilder writeBehindConfigurationBuilder = WriteBehindConfigurationBuilder.newWriteBehindConfiguration();
    WriteBehindConfiguration writeBehindConfiguration = writeBehindConfigurationBuilder.concurrencyLevel(3).batchSize(4)
                                                                                        .queueSize(10)
                                                                                        .build();
    
    builder.using(cacheLoaderWriterFactory);
    ResourcePoolsBuilder resourcePoolsBuilder = newResourcePoolsBuilder()
        .with(org.ehcache.config.ResourceType.Core.HEAP, 10, EntryUnit.ENTRIES, false);
   
    cacheManager = builder.build(true);
    testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withExpiry(Expirations.timeToLiveExpiration(new Duration(100, TimeUnit.MILLISECONDS)))
        .withResourcePools(resourcePoolsBuilder)
        .addServiceConfig(writeBehindConfiguration)
        .buildConfig(String.class, String.class));

  }
  
}
