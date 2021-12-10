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

package org.ehcache.impl.config.persistence;

import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheManagerConfiguration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.core.HumanReadable;

import java.io.File;

/**
 * Convenience configuration type that enables the {@link CacheManagerBuilder} to return a more specific type of
 * {@link CacheManager}, that is a {@link PersistentCacheManager}.
 */
public class CacheManagerPersistenceConfiguration extends DefaultPersistenceConfiguration implements CacheManagerConfiguration<PersistentCacheManager>, HumanReadable {

  /**
   * Creates a new configuration object with the provided parameters.
   *
   * @param rootDirectory the root directory to use for disk storage
   */
  public CacheManagerPersistenceConfiguration(final File rootDirectory) {
    super(rootDirectory);
  }

  /**
   * Transforms the builder received in one that returns a {@link PersistentCacheManager}.
   */
  @Override
  @SuppressWarnings("unchecked")
  public CacheManagerBuilder<PersistentCacheManager> builder(final CacheManagerBuilder<? extends CacheManager> other) {
    return (CacheManagerBuilder<PersistentCacheManager>)other.using(this);
  }

  @Override
  public String readableString() {
    return this.getClass().getName() + ":\n    " +
        "rootDirectory: " + getRootDirectory();
  }
}
