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
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;

import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;

/**
 *
 * @author cdennis
 */
public class PooledExecutorWriteBehindTest extends AbstractWriteBehindTestBase {

  @Override
  protected CacheConfigurationBuilder<Object, Object> configurationBuilder() {
    return newCacheConfigurationBuilder()
            .withExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
  }

  @Override
  protected CacheManagerBuilder managerBuilder() {
    PooledExecutionServiceConfiguration threadPoolConfig = new PooledExecutionServiceConfiguration();
    threadPoolConfig.addDefaultPool("threadpool", 2, 8);
    return newCacheManagerBuilder().using(threadPoolConfig);
  }
}
