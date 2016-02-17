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

package org.ehcache.clustered.config;

import org.ehcache.CacheManager;
import org.ehcache.clustered.ClusteredCacheManager;
import org.ehcache.clustered.ClusteredCacheManagerBuilder;
import org.ehcache.clustered.service.ClusteringService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.CacheManagerConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.net.URI;

/**
 * Specifies the configuration for a {@link ClusteringService}.
 *
 * @author Clifford W. Johnson
 */
// TODO: Should this accept/hold a *list* of URIs?
// TODO: Is 'alias' *required*?
public class ClusteringServiceConfiguration
    implements ServiceCreationConfiguration<ClusteringService>,
    CacheManagerConfiguration<ClusteredCacheManager> {

  private final String alias;
  private final URI connectionUrl;

  public ClusteringServiceConfiguration(final String alias, final URI connectionUrl) {
    if (connectionUrl == null) {
      throw new NullPointerException("connectionUrl");
    }
    this.alias = (alias == null ? "" : alias);
    this.connectionUrl = connectionUrl;
  }

  public String getAlias() {
    return alias;
  }

  public URI getConnectionUrl() {
    return connectionUrl;
  }

  @Override
  public Class<ClusteringService> getServiceType() {
    return ClusteringService.class;
  }

  @Override
  public CacheManagerBuilder<ClusteredCacheManager> builder(final CacheManagerBuilder<? extends CacheManager> other) {
    return ClusteredCacheManagerBuilder.newCacheManagerBuilder(other).using(this);
  }
}
