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

package org.ehcache.config.loaderwriter;

import org.ehcache.internal.classes.ClassInstanceProviderConfig;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterFactory;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
* @author Alex Snaps
*/
public class DefaultCacheLoaderWriterConfiguration extends ClassInstanceProviderConfig<CacheLoaderWriter<?, ?>> implements CacheLoaderWriterConfiguration {

  public DefaultCacheLoaderWriterConfiguration(final Class<? extends CacheLoaderWriter<?, ?>> clazz) {
    super(clazz);
  }

  @Override
  public Class<CacheLoaderWriterFactory> getServiceType() {
    return CacheLoaderWriterFactory.class;
  }

  @Override
  public boolean isWriteBehind() {
    return false;
  };
  
  @Override
  public int minWriteDelay() {
    return 0;
  }

  @Override
  public int maxWriteDelay() {
    return 0;
  }

  @Override
  public int rateLimitPerSecond() {
    return 0;
  }

  @Override
  public boolean writeCoalescing() {
    return false;
  }

  @Override
  public boolean writeBatching() {
    return false;
  }

  @Override
  public int writeBatchSize() {
    return 0;
  }

  @Override
  public int retryAttempts() {
    return 0;
  }

  @Override
  public int retryAttemptDelaySeconds() {
    return 0;
  }

  @Override
  public int writeBehindConcurrency() {
    return 0;
  }

  @Override
  public int writeBehindMaxQueueSize() {
    return 0;
  }
}
