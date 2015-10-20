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

import java.util.concurrent.TimeUnit;
import org.ehcache.CacheManagerBuilder;
import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;

import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;

/**
 * @author Abhilash
 *
 */
public class WriteBehindEvictionTest extends AbstractWriteBehindTestBase {

  @Override
  protected CacheConfigurationBuilder<Object, Object> configurationBuilder() {
    return newCacheConfigurationBuilder()
            .withResourcePools(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
            .withExpiry(Expirations.timeToLiveExpiration(new Duration(100, TimeUnit.MILLISECONDS)));
  }

  @Override
  protected CacheManagerBuilder managerBuilder() {
    return newCacheManagerBuilder();
  }
}
