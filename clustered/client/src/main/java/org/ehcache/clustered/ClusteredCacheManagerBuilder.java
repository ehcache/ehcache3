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

package org.ehcache.clustered;

import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.spi.service.Service;

import java.util.Collection;
import java.util.Set;

/**
 * @author Clifford W. Johnson
 */
public class ClusteredCacheManagerBuilder extends CacheManagerBuilder<ClusteredCacheManager> {

  protected ClusteredCacheManagerBuilder(final CacheManagerBuilder<ClusteredCacheManager> builder, final Set<Service> services) {
    super(builder, services);
  }

  protected ClusteredCacheManagerBuilder(final CacheManagerBuilder<ClusteredCacheManager> builder, final ConfigurationBuilder configBuilder) {
    super(builder, configBuilder);
  }

  private ClusteredCacheManagerBuilder(final CacheManagerBuilder<?> builder) {
    super(builder);
  }

  @Override
  protected ClusteredCacheManager newCacheManager(final Collection<Service> services, final Configuration configuration) {
    return new ClusteredCacheManagerImpl(configuration, services);
  }

  @Override
  protected ClusteredCacheManagerBuilder newBuilder(final Set<Service> services) {
    return new ClusteredCacheManagerBuilder(this, services);
  }

  @Override
  protected ClusteredCacheManagerBuilder newBuilder(final ConfigurationBuilder builder) {
    return new ClusteredCacheManagerBuilder(this, builder);
  }

  public static ClusteredCacheManager newCacheManager(final Configuration configuration) {
    return CacheManagerBuilder.newSpecializedCacheManager(ClusteredCacheManager.class, configuration);
  }

  public static ClusteredCacheManagerBuilder newCacheManagerBuilder(final CacheManagerBuilder<?> builder) {
    return new ClusteredCacheManagerBuilder(builder);
  }
}
