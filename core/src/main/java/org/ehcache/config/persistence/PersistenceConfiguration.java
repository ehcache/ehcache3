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

package org.ehcache.config.persistence;

import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheManagerConfiguration;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.ServiceConfiguration;

import java.io.File;

/**
 * @author Alex Snaps
 */
public class PersistenceConfiguration implements ServiceConfiguration<LocalPersistenceService>, CacheManagerConfiguration<PersistentCacheManager> {

  private final File rootDirectory;

  public PersistenceConfiguration(final File rootDirectory) {
    this.rootDirectory = rootDirectory;
  }

  public File getRootDirectory() {
    return rootDirectory;
  }

  @Override
  public Class<LocalPersistenceService> getServiceType() {
    return LocalPersistenceService.class;
  }

  @Override
  public CacheManagerBuilder<PersistentCacheManager> builder(final CacheManagerBuilder<? extends CacheManager> other) {
    return (CacheManagerBuilder<PersistentCacheManager>)other.using(this);
  }
}
